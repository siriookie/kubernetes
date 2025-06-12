/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handlers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"golang.org/x/net/websocket"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/httpstream/wsstream"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// nothing will ever be sent down this channel
var neverExitWatch <-chan time.Time = make(chan time.Time)

// timeoutFactory abstracts watch timeout logic for testing
type TimeoutFactory interface {
	TimeoutCh() (<-chan time.Time, func() bool)
}

// realTimeoutFactory implements timeoutFactory
type realTimeoutFactory struct {
	timeout time.Duration
}

// TimeoutCh returns a channel which will receive something when the watch times out,
// and a cleanup function to call when this happens.
func (w *realTimeoutFactory) TimeoutCh() (<-chan time.Time, func() bool) {
	if w.timeout == 0 {
		return neverExitWatch, func() bool { return false }
	}
	t := time.NewTimer(w.timeout)
	return t.C, t.Stop
}

// serveWatchHandler returns a handle to serve a watch response.
// 为一次 watch 请求（也就是客户端通过 watch=true 发起的请求）创建一个 HTTP 处理器（http.Handler），用于持续地将资源变更事件从 apiserver 发送到客户端。
// TODO: the functionality in this method and in WatchServer.Serve is not cleanly decoupled.
func serveWatchHandler(watcher watch.Interface, scope *RequestScope, mediaTypeOptions negotiation.MediaTypeOptions, req *http.Request, w http.ResponseWriter, timeout time.Duration, metricsScope string, initialEventsListBlueprint runtime.Object) (http.Handler, error) {
	//根据请求和 negotiated 的媒体类型，解析是否需要对象转换（如 Table 转换）等。
	options, err := optionsForTransform(mediaTypeOptions, req)
	if err != nil {
		return nil, err
	}

	// negotiate for the stream serializer from the scope's serializer
	//决定用什么流媒体编码格式（通常是 JSON，但也可能是 CBOR）。
	serializer, err := negotiation.NegotiateOutputMediaTypeStream(req, scope.Serializer, scope)
	if err != nil {
		return nil, err
	}
	framer := serializer.StreamSerializer.Framer
	var encoder runtime.Encoder
	//根据 negotiated 的编码器，创建实际对象的序列化器。
	if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
		encoder = scope.Serializer.EncoderForVersion(runtime.UseNondeterministicEncoding(serializer.StreamSerializer.Serializer), scope.Kind.GroupVersion())
	} else {
		encoder = scope.Serializer.EncoderForVersion(serializer.StreamSerializer.Serializer, scope.Kind.GroupVersion())
	}
	useTextFraming := serializer.EncodesAsText
	if framer == nil {
		return nil, fmt.Errorf("no framer defined for %q available for embedded encoding", serializer.MediaType)
	}
	// TODO: next step, get back mediaTypeOptions from negotiate and return the exact value here
	mediaType := serializer.MediaType
	//如果是 CBOR 序列化，按照标准指定 application/cbor-seq。
	switch mediaType {
	case runtime.ContentTypeJSON:
		// as-is
	case runtime.ContentTypeCBOR:
		// If a client indicated it accepts application/cbor (exactly one data item) on a
		// watch request, set the conformant application/cbor-seq media type the watch
		// response. RFC 9110 allows an origin server to deviate from the indicated
		// preference rather than send a 406 (Not Acceptable) response (see
		// https://www.rfc-editor.org/rfc/rfc9110.html#section-12.1-5).
		mediaType = runtime.ContentTypeCBORSequence
	default:
		mediaType += ";stream=watch"
	}

	ctx := req.Context()

	// locate the appropriate embedded encoder based on the transform
	var negotiatedEncoder runtime.Encoder
	contentKind, contentSerializer, transform := targetEncodingForTransform(scope, mediaTypeOptions, req)
	if transform {
		info, ok := runtime.SerializerInfoForMediaType(contentSerializer.SupportedMediaTypes(), serializer.MediaType)
		if !ok {
			return nil, fmt.Errorf("no encoder for %q exists in the requested target %#v", serializer.MediaType, contentSerializer)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
			negotiatedEncoder = contentSerializer.EncoderForVersion(runtime.UseNondeterministicEncoding(info.Serializer), contentKind.GroupVersion())
		} else {
			negotiatedEncoder = contentSerializer.EncoderForVersion(info.Serializer, contentKind.GroupVersion())
		}
	} else {
		if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
			negotiatedEncoder = scope.Serializer.EncoderForVersion(runtime.UseNondeterministicEncoding(serializer.Serializer), contentKind.GroupVersion())
		} else {
			negotiatedEncoder = scope.Serializer.EncoderForVersion(serializer.Serializer, contentKind.GroupVersion())
		}
	}

	var memoryAllocator runtime.MemoryAllocator
	//为了优化性能，支持内存复用的 encoder 会使用预分配的 Allocator，避免频繁 GC。
	if encoderWithAllocator, supportsAllocator := negotiatedEncoder.(runtime.EncoderWithAllocator); supportsAllocator {
		// don't put the allocator inside the embeddedEncodeFn as that would allocate memory on every call.
		// instead, we allocate the buffer for the entire watch session and release it when we close the connection.
		memoryAllocator = runtime.AllocatorPool.Get().(*runtime.Allocator)
		negotiatedEncoder = runtime.NewEncoderWithAllocator(encoderWithAllocator, memoryAllocator)
	}
	var tableOptions *metav1.TableOptions
	if options != nil {
		if passedOptions, ok := options.(*metav1.TableOptions); ok {
			tableOptions = passedOptions
		} else {
			return nil, fmt.Errorf("unexpected options type: %T", options)
		}
	}
	embeddedEncoder := newWatchEmbeddedEncoder(ctx, negotiatedEncoder, mediaTypeOptions.Convert, tableOptions, scope)

	if encoderWithAllocator, supportsAllocator := encoder.(runtime.EncoderWithAllocator); supportsAllocator {
		if memoryAllocator == nil {
			// don't put the allocator inside the embeddedEncodeFn as that would allocate memory on every call.
			// instead, we allocate the buffer for the entire watch session and release it when we close the connection.
			memoryAllocator = runtime.AllocatorPool.Get().(*runtime.Allocator)
		}
		encoder = runtime.NewEncoderWithAllocator(encoderWithAllocator, memoryAllocator)
	}

	var serverShuttingDownCh <-chan struct{}
	if signals := apirequest.ServerShutdownSignalFrom(req.Context()); signals != nil {
		serverShuttingDownCh = signals.ShuttingDown()
	}
	//将上面的 encoder、framer、watcher 等打包成一个 WatchServer，它是真正负责发送事件的对象。
	server := &WatchServer{
		Watching: watcher,
		Scope:    scope,

		UseTextFraming:  useTextFraming,
		MediaType:       mediaType,
		Framer:          framer,
		Encoder:         encoder,
		EmbeddedEncoder: embeddedEncoder,

		watchListTransformerFn: newWatchListTransformer(initialEventsListBlueprint, mediaTypeOptions.Convert, negotiatedEncoder).transform,

		MemoryAllocator:      memoryAllocator,
		TimeoutFactory:       &realTimeoutFactory{timeout},
		ServerShuttingDownCh: serverShuttingDownCh,

		metricsScope: metricsScope,
	}
	//Kubernetes watch 支持 WebSocket 和 HTTP 连接，这里根据请求类型选择返回 WebSocket 处理器还是普通 HTTP 处理器。
	if wsstream.IsWebSocketRequest(req) {
		w.Header().Set("Content-Type", server.MediaType)
		return websocket.Handler(server.HandleWS), nil
	}
	return http.HandlerFunc(server.HandleHTTP), nil
}

// WatchServer serves a watch.Interface over a websocket or vanilla HTTP.
type WatchServer struct {
	Watching watch.Interface
	Scope    *RequestScope

	// true if websocket messages should use text framing (as opposed to binary framing)
	UseTextFraming bool
	// the media type this watch is being served with
	MediaType string
	// used to frame the watch stream
	Framer runtime.Framer
	// used to encode the watch stream event itself
	Encoder runtime.Encoder
	// used to encode the nested object in the watch stream
	EmbeddedEncoder runtime.Encoder
	// watchListTransformerFn a function applied
	// to watchlist bookmark events that transforms
	// the embedded object before sending it to a client.
	watchListTransformerFn watchListTransformerFunction

	MemoryAllocator      runtime.MemoryAllocator
	TimeoutFactory       TimeoutFactory
	ServerShuttingDownCh <-chan struct{}

	metricsScope string
}

// HandleHTTP serves a series of encoded events via HTTP with Transfer-Encoding: chunked.
// or over a websocket connection.
func (s *WatchServer) HandleHTTP(w http.ResponseWriter, req *http.Request) {
	//如果使用了内存池分配器，结束时归还给池子，避免内存泄露。
	defer func() {
		if s.MemoryAllocator != nil {
			runtime.AllocatorPool.Put(s.MemoryAllocator)
		}
	}()
	//watch 是基于 HTTP 长连接 + chunked 编码，要求底层能主动 flush 内容（把数据推给客户端）。不支持就返回 500 错误。
	flusher, ok := w.(http.Flusher)
	if !ok {
		err := fmt.Errorf("unable to start watch - can't get http.Flusher: %#v", w)
		utilruntime.HandleError(err)
		s.Scope.err(errors.NewInternalError(err), w, req)
		return
	}
	//帧写入器用于把一个个 watch 事件写入响应流中（如 JSON 每一行是一个事件）。如果不支持 framing，则返回错误。
	framer := s.Framer.NewFrameWriter(w)
	if framer == nil {
		// programmer error
		err := fmt.Errorf("no stream framing support is available for media type %q", s.MediaType)
		utilruntime.HandleError(err)
		s.Scope.err(errors.NewBadRequest(err.Error()), w, req)
		return
	}

	// ensure the connection times out
	//创建一个超时通道，watch 不能无限连接，默认有一个最长生命周期，比如 5 分钟。
	timeoutCh, cleanup := s.TimeoutFactory.TimeoutCh()
	defer cleanup()

	// begin the stream
	//告诉客户端我们将以流式方式返回内容。
	w.Header().Set("Content-Type", s.MediaType)
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	//watchEncoder 负责把 watch.Event 编码成 JSON 等格式。
	//ch 是事件通道，里面源源不断地收到资源的变化（由 etcd watch 或 watchcache 提供）。
	kind := s.Scope.Kind
	watchEncoder := newWatchEncoder(req.Context(), kind, s.EmbeddedEncoder, s.Encoder, framer, s.watchListTransformerFn)
	ch := s.Watching.ResultChan()
	done := req.Context().Done()
	//🔌 ServerShuttingDownCh: apiserver 要关闭了，终止。
	//
	//❌ done: 客户端主动断开连接。
	//
	//⏳ timeoutCh: 达到连接最长存活时间。
	//
	//📡 event, ok := <-ch: 收到事件。发送给客户端。
	for {
		select {
		case <-s.ServerShuttingDownCh:
			// the server has signaled that it is shutting down (not accepting
			// any new request), all active watch request(s) should return
			// immediately here. The WithWatchTerminationDuringShutdown server
			// filter will ensure that the response to the client is rate
			// limited in order to avoid any thundering herd issue when the
			// client(s) try to reestablish the WATCH on the other
			// available apiserver instance(s).
			return
		case <-done:
			return
		case <-timeoutCh:
			return
		case event, ok := <-ch:
			if !ok {
				// End of results.
				return
			}
			metrics.WatchEvents.WithContext(req.Context()).WithLabelValues(kind.Group, kind.Version, kind.Kind).Inc()
			isWatchListLatencyRecordingRequired := shouldRecordWatchListLatency(event)

			if err := watchEncoder.Encode(event); err != nil {
				utilruntime.HandleError(err)
				// client disconnect.
				return
			}

			if len(ch) == 0 {
				flusher.Flush()
			}
			if isWatchListLatencyRecordingRequired {
				metrics.RecordWatchListLatency(req.Context(), s.Scope.Resource, s.metricsScope)
			}
		}
	}
}

// HandleWS serves a series of encoded events over a websocket connection.
func (s *WatchServer) HandleWS(ws *websocket.Conn) {
	defer func() {
		if s.MemoryAllocator != nil {
			runtime.AllocatorPool.Put(s.MemoryAllocator)
		}
	}()

	defer ws.Close()
	done := make(chan struct{})
	// ensure the connection times out
	timeoutCh, cleanup := s.TimeoutFactory.TimeoutCh()
	defer cleanup()

	go func() {
		defer utilruntime.HandleCrash()
		// This blocks until the connection is closed.
		// Client should not send anything.
		wsstream.IgnoreReceives(ws, 0)
		// Once the client closes, we should also close
		close(done)
	}()

	framer := newWebsocketFramer(ws, s.UseTextFraming)

	kind := s.Scope.Kind
	watchEncoder := newWatchEncoder(context.TODO(), kind, s.EmbeddedEncoder, s.Encoder, framer, s.watchListTransformerFn)
	ch := s.Watching.ResultChan()

	for {
		select {
		case <-done:
			return
		case <-timeoutCh:
			return
		case event, ok := <-ch:
			if !ok {
				// End of results.
				return
			}

			if err := watchEncoder.Encode(event); err != nil {
				utilruntime.HandleError(err)
				// client disconnect.
				return
			}
		}
	}
}

type websocketFramer struct {
	ws             *websocket.Conn
	useTextFraming bool
}

func newWebsocketFramer(ws *websocket.Conn, useTextFraming bool) io.Writer {
	return &websocketFramer{
		ws:             ws,
		useTextFraming: useTextFraming,
	}
}

func (w *websocketFramer) Write(p []byte) (int, error) {
	if w.useTextFraming {
		// bytes.Buffer::String() has a special handling of nil value, but given
		// we're writing serialized watch events, this will never happen here.
		if err := websocket.Message.Send(w.ws, string(p)); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	if err := websocket.Message.Send(w.ws, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

var _ io.Writer = &websocketFramer{}

func shouldRecordWatchListLatency(event watch.Event) bool {
	if event.Type != watch.Bookmark || !utilfeature.DefaultFeatureGate.Enabled(features.WatchList) {
		return false
	}
	// as of today the initial-events-end annotation is added only to a single event
	// by the watch cache and only when certain conditions are met
	//
	// for more please read https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/3157-watch-list
	hasAnnotation, err := storage.HasInitialEventsEndBookmarkAnnotation(event.Object)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to determine if the obj has the required annotation for measuring watchlist latency, obj %T: %v", event.Object, err))
		return false
	}
	return hasAnnotation
}
