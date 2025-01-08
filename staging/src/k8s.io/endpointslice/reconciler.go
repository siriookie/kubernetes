/*
Copyright 2019 The Kubernetes Authors.

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

package endpointslice

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/endpointslice/metrics"
	"k8s.io/endpointslice/topologycache"
	"k8s.io/endpointslice/trafficdist"
	endpointsliceutil "k8s.io/endpointslice/util"
	"k8s.io/klog/v2"
)

// Reconciler is responsible for transforming current EndpointSlice state into
// desired state
type Reconciler struct {
	client               clientset.Interface
	nodeLister           corelisters.NodeLister
	maxEndpointsPerSlice int32
	endpointSliceTracker *endpointsliceutil.EndpointSliceTracker
	metricsCache         *metrics.Cache
	// topologyCache tracks the distribution of Nodes and endpoints across zones
	// to enable TopologyAwareHints.
	topologyCache *topologycache.TopologyCache
	// trafficDistributionEnabled determines if endpointDistribution field is to
	// be considered when reconciling EndpointSlice hints.
	trafficDistributionEnabled bool
	// eventRecorder allows Reconciler to record and publish events.
	eventRecorder  record.EventRecorder
	controllerName string
}

type ReconcilerOption func(*Reconciler)

// WithTrafficDistributionEnabled controls whether the Reconciler considers the
// `trafficDistribution` field while reconciling EndpointSlices.
func WithTrafficDistributionEnabled(enabled bool) ReconcilerOption {
	return func(r *Reconciler) {
		r.trafficDistributionEnabled = enabled
	}
}

// endpointMeta includes the attributes we group slices on, this type helps with
// that logic in Reconciler
type endpointMeta struct {
	ports       []discovery.EndpointPort
	addressType discovery.AddressType
}

// Reconcile takes a set of pods currently matching a service selector and
// compares them with the endpoints already present in any existing endpoint
// slices for the given service. It creates, updates, or deletes endpoint slices
// to ensure the desired set of pods are represented by endpoint slices.
// Reconcile（调和）是一个函数，它检查当前匹配某个服务选择器（Service Selector）的所有 Pods。
// 它将这些 Pods 与给定服务已经存在的 EndpointSlices 中的 Endpoints 进行对比。
// 根据比较结果，Reconcile 会创建、更新或者删除这些 EndpointSlices，以确保服务中所期望的 Pods 被正确地表示在 EndpointSlices 中。
func (r *Reconciler) Reconcile(logger klog.Logger, service *corev1.Service, pods []*corev1.Pod, existingSlices []*discovery.EndpointSlice, triggerTime time.Time) error {
	slicesToDelete := []*discovery.EndpointSlice{}                                    // slices that are no longer  matching any address the service has
	errs := []error{}                                                                 // all errors generated in the process of reconciling
	slicesByAddressType := make(map[discovery.AddressType][]*discovery.EndpointSlice) // slices by address type

	// addresses that this service supports [o(1) find]
	// 拿 到 service 支 持 的 ip 地 址， 放 进 set 里
	serviceSupportedAddressesTypes := getAddressTypesForService(logger, service)

	// loop through slices identifying their address type.
	// slices that no longer match address type supported by services
	// go to delete, other slices goes to the Reconciler machinery
	// for further adjustment
	// 遍 历 service 的 existingSlices，碰 到 Slice的AddressType 不 在 service 支 持 的 AddressType 内 的
	// 如 果 有 topologyCache ，就 要 在 topologyCache 里 删 除 这  个 service 的 相 关 信 息
	// 把 slice 加 到 slicesToDelete 中 ， 最 后 要 删 除 的
	for _, existingSlice := range existingSlices {
		// service no longer supports that address type, add it to deleted slices
		if !serviceSupportedAddressesTypes.Has(existingSlice.AddressType) {
			if r.topologyCache != nil {
				svcKey, err := ServiceControllerKey(existingSlice)
				if err != nil {
					logger.Info("Couldn't get key to remove EndpointSlice from topology cache", "existingSlice", existingSlice, "err", err)
				} else {
					r.topologyCache.RemoveHints(svcKey, existingSlice.AddressType)
				}
			}

			slicesToDelete = append(slicesToDelete, existingSlice)
			continue
		}

		// add list if it is not on our map
		if _, ok := slicesByAddressType[existingSlice.AddressType]; !ok {
			slicesByAddressType[existingSlice.AddressType] = make([]*discovery.EndpointSlice, 0, 1)
		}
		// 如 果 存 在 这 个 AddressType ， 就 加 入  map
		slicesByAddressType[existingSlice.AddressType] = append(slicesByAddressType[existingSlice.AddressType], existingSlice)
	}

	// reconcile for existing.
	// 遍 历 serviceSupportedAddressesTypes ， 找 到 每 种  type 对 应 的 existingSlices
	for addressType := range serviceSupportedAddressesTypes {
		existingSlices := slicesByAddressType[addressType]
		err := r.reconcileByAddressType(logger, service, pods, existingSlices, triggerTime, addressType)
		if err != nil {
			errs = append(errs, err)
		}
	}

	// delete those which are of addressType that is no longer supported
	// by the service
	for _, sliceToDelete := range slicesToDelete {
		err := r.client.DiscoveryV1().EndpointSlices(service.Namespace).Delete(context.TODO(), sliceToDelete.Name, metav1.DeleteOptions{})
		if err != nil {
			errs = append(errs, fmt.Errorf("error deleting %s EndpointSlice for Service %s/%s: %w", sliceToDelete.Name, service.Namespace, service.Name, err))
		} else {
			r.endpointSliceTracker.ExpectDeletion(sliceToDelete)
			metrics.EndpointSliceChanges.WithLabelValues("delete").Inc()
		}
	}

	return utilerrors.NewAggregate(errs)
}

// reconcileByAddressType takes a set of pods currently matching a service selector and
// compares them with the endpoints already present in any existing endpoint
// slices (by address type) for the given service. It creates, updates, or deletes endpoint slices
// to ensure the desired set of pods are represented by endpoint slices.
// reconcileByAddressType 获取当前与服务选择器匹配的 Pod 集合，并与给定服务下已有的 EndpointSlice（按照地址类型分类）进行比较。
// 它会 创建、更新 或 删除 EndpointSlice，以确保所有期望的 Pod 都能被正确地映射到相应的 EndpointSlice 中。
func (r *Reconciler) reconcileByAddressType(logger klog.Logger, service *corev1.Service, pods []*corev1.Pod, existingSlices []*discovery.EndpointSlice, triggerTime time.Time, addressType discovery.AddressType) error {
	errs := []error{}

	slicesToCreate := []*discovery.EndpointSlice{}
	slicesToUpdate := []*discovery.EndpointSlice{}
	slicesToDelete := []*discovery.EndpointSlice{}
	events := []*topologycache.EventBuilder{}

	// Build data structures for existing state.
	// 根 据 port 的 hash 创 建 一 个 map，value 是 slice 的 list
	//{
	//
	//    "ephash":[{apiVersion: discovery.k8s.io/v1
	//kind: EndpointSlice
	//metadata:
	//  name: my-service-abcdef123456
	//  namespace: default
	//  labels:
	//    kubernetes.io/service-name: my-service
	//addressType: IPv4
	//endpoints:
	//  - addresses:
	//      - 10.1.1.10
	//    conditions:
	//      ready: true
	//  - addresses:
	//      - 10.1.1.11
	//    conditions:
	//      ready: true
	//  - addresses:
	//      - 10.1.1.12
	//    conditions:
	//      ready: false # 表示这个 Pod 不处于 ready 状态
	//ports:
	//  - name: http
	//    protocol: TCP
	//    port: 80
	//        },{}]
	//    //ephash likes : ports:
	//                        - name: http
	//                            protocol: TCP
	//                            port: 80
	//}
	existingSlicesByPortMap := map[endpointsliceutil.PortMapKey][]*discovery.EndpointSlice{}
	for _, existingSlice := range existingSlices {
		if ownedBy(existingSlice, service) {
			/* ports:
			- name: http
			  protocol: TCP
			  port: 80
			*/
			// 把 上 面 这 种数 据  进 行 hash
			epHash := endpointsliceutil.NewPortMapKey(existingSlice.Ports)
			//{epHash:[ *v1.EndpointSlice, *v1.EndpointSlice, *v1.EndpointSlice]}
			existingSlicesByPortMap[epHash] = append(existingSlicesByPortMap[epHash], existingSlice)
		} else {
			// 能 根 据 service  查 出 来  endpointsclice 但 是 这 个  endpointslice 的 owner 不 是 这 个 service 时 才 会
			slicesToDelete = append(slicesToDelete, existingSlice)
		}
	}

	// Build data structures for desired state.
	desiredMetaByPortMap := map[endpointsliceutil.PortMapKey]*endpointMeta{}
	desiredEndpointsByPortMap := map[endpointsliceutil.PortMapKey]endpointsliceutil.EndpointSet{}
	//  循 环 遍 历  pod 创建出EndpointSet
	for _, pod := range pods {
		// 去 除 掉 phase == v1.PodFailed || phase == v1.PodSucceeded 的 pod 和
		// 没 有 ip 地 址 的 pod
		if !endpointsliceutil.ShouldPodBeInEndpoints(pod, true) {
			continue
		}

		endpointPorts := getEndpointPorts(logger, service, pod)
		// 把 endpointPorts 进 行 hash 存 进 desiredEndpointsByPortMap
		epHash := endpointsliceutil.NewPortMapKey(endpointPorts)
		if _, ok := desiredEndpointsByPortMap[epHash]; !ok {
			desiredEndpointsByPortMap[epHash] = endpointsliceutil.EndpointSet{}
		}
		// 把 endpointPorts 进 行 hash 存 进 desiredMetaByPortMap
		if _, ok := desiredMetaByPortMap[epHash]; !ok {
			desiredMetaByPortMap[epHash] = &endpointMeta{
				addressType: addressType,
				ports:       endpointPorts,
			}
		}

		node, err := r.nodeLister.Get(pod.Spec.NodeName)
		if err != nil {
			// we are getting the information from the local informer,
			// an error different than IsNotFound should not happen
			if !errors.IsNotFound(err) {
				return err
			}
			// If the Node specified by the Pod doesn't exist we want to requeue the Service so we
			// retry later, but also update the EndpointSlice without the problematic Pod.
			// Theoretically, the pod Garbage Collector will remove the Pod, but we want to avoid
			// situations where a reference from a Pod to a missing node can leave the EndpointSlice
			// stuck forever.
			// On the other side, if the service.Spec.PublishNotReadyAddresses is set we just add the
			// Pod, since the user is explicitly indicating that the Pod address should be published.
			if !service.Spec.PublishNotReadyAddresses {
				logger.Info("skipping Pod for Service, Node not found", "pod", klog.KObj(pod), "service", klog.KObj(service), "node", klog.KRef("", pod.Spec.NodeName))
				errs = append(errs, fmt.Errorf("skipping Pod %s for Service %s/%s: Node %s Not Found", pod.Name, service.Namespace, service.Name, pod.Spec.NodeName))
				continue
			}
		}
		// 生 成 endpoint 对 象
		endpoint := podToEndpoint(pod, node, service, addressType)
		if len(endpoint.Addresses) > 0 {
			desiredEndpointsByPortMap[epHash].Insert(&endpoint)
		}
	}

	spMetrics := metrics.NewServicePortCache()
	totalAdded := 0
	totalRemoved := 0

	// Determine changes necessary for each group of slices by port map.
	// 遍 历 desiredEndpointsByPortMap
	for portMap, desiredEndpoints := range desiredEndpointsByPortMap {
		numEndpoints := len(desiredEndpoints)
		// 拿 到 每 个 port 对 应 的 需 要 创 建 的 、 更 新 的 、 删 除 的  endpointSlices
		pmSlicesToCreate, pmSlicesToUpdate, pmSlicesToDelete, added, removed := r.reconcileByPortMapping(
			logger, service, existingSlicesByPortMap[portMap], desiredEndpoints, desiredMetaByPortMap[portMap])

		totalAdded += added
		totalRemoved += removed

		spMetrics.Set(portMap, metrics.EfficiencyInfo{
			Endpoints: numEndpoints,
			Slices:    len(existingSlicesByPortMap[portMap]) + len(pmSlicesToCreate) - len(pmSlicesToDelete),
		})

		slicesToCreate = append(slicesToCreate, pmSlicesToCreate...)
		slicesToUpdate = append(slicesToUpdate, pmSlicesToUpdate...)
		slicesToDelete = append(slicesToDelete, pmSlicesToDelete...)
	}

	// If there are unique sets of ports that are no longer desired, mark
	// the corresponding endpoint slices for deletion.
	for portMap, existingSlices := range existingSlicesByPortMap {
		if _, ok := desiredEndpointsByPortMap[portMap]; !ok {
			slicesToDelete = append(slicesToDelete, existingSlices...)
		}
	}

	// When no endpoint slices would usually exist, we need to add a placeholder.
	//这些条件组合意味着服务当前处于 没有可用端点 的状态，且操作后将不存在 EndpointSlice。
	//但为了避免该服务完全没有 EndpointSlice，需要创建一个占位符。
	if len(existingSlices) == len(slicesToDelete) && len(slicesToCreate) < 1 {
		// Check for existing placeholder slice outside of the core control flow
		placeholderSlice := newEndpointSlice(logger, service, &endpointMeta{ports: []discovery.EndpointPort{}, addressType: addressType}, r.controllerName)
		if len(slicesToDelete) == 1 && placeholderSliceCompare.DeepEqual(slicesToDelete[0], placeholderSlice) {
			// We are about to unnecessarily delete/recreate the placeholder, remove it now.
			slicesToDelete = slicesToDelete[:0]
		} else {
			slicesToCreate = append(slicesToCreate, placeholderSlice)
		}
		spMetrics.Set(endpointsliceutil.NewPortMapKey(placeholderSlice.Ports), metrics.EfficiencyInfo{
			Endpoints: 0,
			Slices:    1,
		})
	}

	metrics.EndpointsAddedPerSync.WithLabelValues().Observe(float64(totalAdded))
	metrics.EndpointsRemovedPerSync.WithLabelValues().Observe(float64(totalRemoved))

	serviceNN := types.NamespacedName{Name: service.Name, Namespace: service.Namespace}
	r.metricsCache.UpdateServicePortCache(serviceNN, spMetrics)

	// Topology hints are assigned per address type. This means it is
	// theoretically possible for endpoints of one address type to be assigned
	// hints while another endpoints of another address type are not.
	//这段代码在 EndpointSlice 控制器的拓扑缓存中：
	//
	//为当前服务和地址类型提供清晰的 EndpointSlice 操作信息。
	//将 EndpointSlice 的状态分类（新增、更新、不变），为后续调度或拓扑优化提供基础数据。
	//确保拓扑提示可以有针对性地作用于特定地址类型的端点。
	si := &topologycache.SliceInfo{
		ServiceKey:  fmt.Sprintf("%s/%s", service.Namespace, service.Name),
		AddressType: addressType,
		ToCreate:    slicesToCreate,
		ToUpdate:    slicesToUpdate,
		Unchanged:   unchangedSlices(existingSlices, slicesToUpdate, slicesToDelete),
	}

	canUseTrafficDistribution := r.trafficDistributionEnabled && !hintsEnabled(service.Annotations)

	// Check if we need to add/remove hints based on the topology annotation.
	//
	// This if/else clause can be removed once the annotation has been deprecated.
	// Ref: https://github.com/kubernetes/enhancements/tree/master/keps/sig-network/4444-service-routing-preference
	//检查服务的拓扑注释是否启用了拓扑提示功能
	if r.topologyCache != nil && hintsEnabled(service.Annotations) {
		// Reaching this point means that we need to configure hints based on the
		// topology annotation.
		slicesToCreate, slicesToUpdate, events = r.topologyCache.AddHints(logger, si)
	} else {
		// Reaching this point means that we will not be configuring hints based on
		// the topology annotation. We need to do 2 things:
		//  1. If hints were added previously based on the annotation, we need to
		//     clear up any locally cached hints from the topologyCache object.
		//  2. Optionally remove the actual hints from the EndpointSlice if we know
		//     that the `trafficDistribution` field is also NOT being used. In other
		//     words, if we know that the `trafficDistribution` field has been
		//     correctly configured by the customer, we DO NOT remove the hints and
		//     wait for the trafficDist handlers to correctly configure them. Always
		//     unconditionally removing hints here (and letting them get readded by
		//     the trafficDist) adds extra overhead in the form of DeepCopy (done
		//     within topologyCache.RemoveHints)

		// Check 1.
		//清理之前添加的提示（Hints）：
		//
		//如果拓扑缓存 (topologyCache) 存在且之前已经基于拓扑注释添加了区域提示，首先会清理这些缓存的提示。r.topologyCache.HasPopulatedHints 会检查该服务的拓扑提示是否已经被填充。如果填充过，则会触发清除提示的操作，并记录事件告警 (EventBuilder)，提示拓扑注释已更改，导致这些提示需要被移除。
		//接着，调用 r.topologyCache.RemoveHints 去移除之前添加的区域提示，防止不再需要的提示干扰后续配置。
		//可选地移除 EndpointSlice 中的实际提示：
		//
		//如果 trafficDistribution 字段不再使用，系统也会尝试从 EndpointSlice 中移除这些提示。这是在 trafficDistribution 配置没有被使用的情况下，移除通过拓扑注释配置的区域提示。如果配置了 trafficDistribution，则不删除提示，避免不必要的删除与再创建过程，因为这会增加额外的负担。
		//
		//对于移除提示的实际操作，调用了 topologycache.RemoveHintsFromSlices(si)，它会根据当前的条件移除指定 EndpointSlice 中的提示。
		if r.topologyCache != nil {
			if r.topologyCache.HasPopulatedHints(si.ServiceKey) {
				logger.Info("TopologyAwareHints annotation has changed, removing hints", "serviceKey", si.ServiceKey, "addressType", si.AddressType)
				events = append(events, &topologycache.EventBuilder{
					EventType: corev1.EventTypeWarning,
					Reason:    "TopologyAwareHintsDisabled",
					Message:   topologycache.FormatWithAddressType(topologycache.TopologyAwareHintsDisabled, si.AddressType),
				})
			}
			r.topologyCache.RemoveHints(si.ServiceKey, addressType)
		}

		// Check 2.
		if !canUseTrafficDistribution {
			slicesToCreate, slicesToUpdate = topologycache.RemoveHintsFromSlices(si)
		}
	}

	if canUseTrafficDistribution {
		r.metricsCache.UpdateTrafficDistributionForService(serviceNN, service.Spec.TrafficDistribution)
		slicesToCreate, slicesToUpdate, _ = trafficdist.ReconcileHints(service.Spec.TrafficDistribution, slicesToCreate, slicesToUpdate, unchangedSlices(existingSlices, slicesToUpdate, slicesToDelete))
	} else {
		r.metricsCache.UpdateTrafficDistributionForService(serviceNN, nil)
	}

	err := r.finalize(service, slicesToCreate, slicesToUpdate, slicesToDelete, triggerTime)
	if err != nil {
		errs = append(errs, err)
	}
	for _, event := range events {
		r.eventRecorder.Event(service, event.EventType, event.Reason, event.Message)
	}
	return utilerrors.NewAggregate(errs)

}

func NewReconciler(client clientset.Interface, nodeLister corelisters.NodeLister, maxEndpointsPerSlice int32, endpointSliceTracker *endpointsliceutil.EndpointSliceTracker, topologyCache *topologycache.TopologyCache, eventRecorder record.EventRecorder, controllerName string, options ...ReconcilerOption) *Reconciler {
	r := &Reconciler{
		client:               client,
		nodeLister:           nodeLister,
		maxEndpointsPerSlice: maxEndpointsPerSlice,
		endpointSliceTracker: endpointSliceTracker,
		metricsCache:         metrics.NewCache(maxEndpointsPerSlice),
		topologyCache:        topologyCache,
		eventRecorder:        eventRecorder,
		controllerName:       controllerName,
	}
	for _, option := range options {
		option(r)
	}
	return r
}

// placeholderSliceCompare is a conversion func for comparing two placeholder endpoint slices.
// It only compares the specific fields we care about.
var placeholderSliceCompare = conversion.EqualitiesOrDie(
	func(a, b metav1.OwnerReference) bool {
		return a.String() == b.String()
	},
	func(a, b metav1.ObjectMeta) bool {
		if a.Namespace != b.Namespace {
			return false
		}
		for k, v := range a.Labels {
			if b.Labels[k] != v {
				return false
			}
		}
		for k, v := range b.Labels {
			if a.Labels[k] != v {
				return false
			}
		}
		return true
	},
)

// finalize creates, updates, and deletes slices as specified
func (r *Reconciler) finalize(
	service *corev1.Service,
	slicesToCreate,
	slicesToUpdate,
	slicesToDelete []*discovery.EndpointSlice,
	triggerTime time.Time,
) error {
	// If there are slices to create and delete, change the creates to updates
	// of the slices that would otherwise be deleted.
	for i := 0; i < len(slicesToDelete); {
		if len(slicesToCreate) == 0 {
			break
		}
		sliceToDelete := slicesToDelete[i]
		slice := slicesToCreate[len(slicesToCreate)-1]
		// Only update EndpointSlices that are owned by this Service and have
		// the same AddressType. We need to avoid updating EndpointSlices that
		// are being garbage collected for an old Service with the same name.
		// The AddressType field is immutable. Since Services also consider
		// IPFamily immutable, the only case where this should matter will be
		// the migration from IP to IPv4 and IPv6 AddressTypes, where there's a
		// chance EndpointSlices with an IP AddressType would otherwise be
		// updated to IPv4 or IPv6 without this check.
		if sliceToDelete.AddressType == slice.AddressType && ownedBy(sliceToDelete, service) {
			slice.Name = sliceToDelete.Name
			slicesToCreate = slicesToCreate[:len(slicesToCreate)-1]
			slicesToUpdate = append(slicesToUpdate, slice)
			slicesToDelete = append(slicesToDelete[:i], slicesToDelete[i+1:]...)
		} else {
			i++
		}
	}
	// Don't create new EndpointSlices if the Service is pending deletion. This
	// is to avoid a potential race condition with the garbage collector where
	// it tries to delete EndpointSlices as this controller replaces them.
	if service.DeletionTimestamp == nil {
		for _, endpointSlice := range slicesToCreate {
			addTriggerTimeAnnotation(endpointSlice, triggerTime)
			createdSlice, err := r.client.DiscoveryV1().EndpointSlices(service.Namespace).Create(context.TODO(), endpointSlice, metav1.CreateOptions{})
			if err != nil {
				// If the namespace is terminating, creates will continue to fail. Simply drop the item.
				if errors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
					return nil
				}
				return fmt.Errorf("failed to create EndpointSlice for Service %s/%s: %v", service.Namespace, service.Name, err)
			}
			r.endpointSliceTracker.Update(createdSlice)
			metrics.EndpointSliceChanges.WithLabelValues("create").Inc()
		}
	}

	for _, endpointSlice := range slicesToUpdate {
		addTriggerTimeAnnotation(endpointSlice, triggerTime)
		updatedSlice, err := r.client.DiscoveryV1().EndpointSlices(service.Namespace).Update(context.TODO(), endpointSlice, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update %s EndpointSlice for Service %s/%s: %v", endpointSlice.Name, service.Namespace, service.Name, err)
		}
		r.endpointSliceTracker.Update(updatedSlice)
		metrics.EndpointSliceChanges.WithLabelValues("update").Inc()
	}

	for _, endpointSlice := range slicesToDelete {
		err := r.client.DiscoveryV1().EndpointSlices(service.Namespace).Delete(context.TODO(), endpointSlice.Name, metav1.DeleteOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete %s EndpointSlice for Service %s/%s: %v", endpointSlice.Name, service.Namespace, service.Name, err)
		}
		r.endpointSliceTracker.ExpectDeletion(endpointSlice)
		metrics.EndpointSliceChanges.WithLabelValues("delete").Inc()
	}

	topologyLabel := "Disabled"
	if r.topologyCache != nil && hintsEnabled(service.Annotations) {
		topologyLabel = "Auto"
	}
	var trafficDistribution string
	if r.trafficDistributionEnabled && !hintsEnabled(service.Annotations) {
		if service.Spec.TrafficDistribution != nil && *service.Spec.TrafficDistribution == corev1.ServiceTrafficDistributionPreferClose {
			trafficDistribution = *service.Spec.TrafficDistribution
		}
	}

	numSlicesChanged := len(slicesToCreate) + len(slicesToUpdate) + len(slicesToDelete)
	metrics.EndpointSlicesChangedPerSync.WithLabelValues(topologyLabel, trafficDistribution).Observe(float64(numSlicesChanged))

	return nil
}

// reconcileByPortMapping compares the endpoints found in existing slices with
// the list of desired endpoints and returns lists of slices to create, update,
// and delete. It also checks that the slices mirror the parent services labels.
// The logic is split up into several main steps:
//  1. Iterate through existing slices, delete endpoints that are no longer
//     desired and update matching endpoints that have changed. It also checks
//     if the slices have the labels of the parent services, and updates them if not.
//  2. Iterate through slices that have been modified in 1 and fill them up with
//     any remaining desired endpoints.
//  3. If there still desired endpoints left, try to fit them into a previously
//     unchanged slice and/or create new ones.
//
// 迭 代 现 有 的  EndpointSlices：
//
// 删 除 那 些 不 再 需 要 的 端 点；
// 更 新 那 些 发 生 变 化 的 端 点；
// 检 查 每 个 Slice 的 标 签 是 否 与 其 父 级  Service 一 致， 不 一 致 时 进 行 修 复。
// 填 充 已 修 改 的 Slices：
//
// 将 第 一 步 中 剩 余 的 （ 未 填 充 的 ） 期 望 端 点 填 入 已 修 改 的  Slices， 最 大 化 复 用 现 有 资 源。
// 复 用 或 创 建  Slices：
//
// 如 果 还 有 多 余 的 端 点 未 被 分 配 到 任 何  Slice，尝 试 将 它 们 加 入 未 被 修 改 的 Slice；
// 若  仍 然 无 法 容 纳 ，则 创 建 新 的 EndpointSlices。
// existingSlices： 通 过 select labels 在 本 地 缓 存 中 直 接 select 出 来 的 EndpointSlice
// desiredSet： 根 据 service 的 现 状 和  现 有 的 pod 组 装 出 来 的 EndpointSlice
func (r *Reconciler) reconcileByPortMapping(
	logger klog.Logger,
	service *corev1.Service,
	existingSlices []*discovery.EndpointSlice,
	desiredSet endpointsliceutil.EndpointSet,
	endpointMeta *endpointMeta,
) ([]*discovery.EndpointSlice, []*discovery.EndpointSlice, []*discovery.EndpointSlice, int, int) {
	slicesByName := map[string]*discovery.EndpointSlice{}
	sliceNamesUnchanged := sets.New[string]()
	sliceNamesToUpdate := sets.New[string]()
	sliceNamesToDelete := sets.New[string]()
	numRemoved := 0

	// 1. Iterate through existing slices to delete endpoints no longer desired
	//    and update endpoints that have changed
	//  遍 历 通 过 select labels 在 本 地 缓 存 中 直 接 select 出 来 的 EndpointSlices
	for _, existingSlice := range existingSlices {
		slicesByName[existingSlice.Name] = existingSlice
		newEndpoints := []discovery.Endpoint{}
		endpointUpdated := false
		// 遍 历 通 过 select labels 在 本 地 缓 存 中 直 接 select 出 来 的 EndpointSlice
		for _, endpoint := range existingSlice.Endpoints {
			// 检 查 endpoint 是 否 存 在 于 根 据 service 的 现 状 和  现 有 的 pod 组 装 出 来 的 EndpointSlice 中
			got := desiredSet.Get(&endpoint)
			// If endpoint is desired add it to list of endpoints to keep.
			if got != nil {
				// 找 到 了， 说 明 没 有 被 删 除
				newEndpoints = append(newEndpoints, *got)
				// If existing version of endpoint doesn't match desired version
				// set endpointUpdated to ensure endpoint changes are persisted.
				// 比 较 除 了 hash 的 字 段 之 外 的 字 段
				if !endpointsliceutil.EndpointsEqualBeyondHash(got, &endpoint) {
					endpointUpdated = true
				}
				// once an endpoint has been placed/found in a slice, it no
				// longer needs to be handled
				// 找 到 之 后 就 不 需 要 再 存 在 desiredSet 中 了，因 为 加入到了newEndpoints，下 面 会 处 理 这 个 endpoint
				desiredSet.Delete(&endpoint)
			}
		}

		// generate the slice labels and check if parent labels have changed
		labels, labelsChanged := setEndpointSliceLabels(logger, existingSlice, service, r.controllerName)

		// If an endpoint was updated or removed, mark for update or delete
		// Endpoints 发 生 了 更 新 或 者 删 除
		if endpointUpdated || len(existingSlice.Endpoints) != len(newEndpoints) {
			// 老 的 大 于 新 的 ， 说 明 有 Endpoint 被 删 除 了
			if len(existingSlice.Endpoints) > len(newEndpoints) {
				numRemoved += len(existingSlice.Endpoints) - len(newEndpoints)
			}
			// 说 明 整 个 Endpoints 都 被 删 除 了
			if len(newEndpoints) == 0 {
				// if no endpoints desired in this slice, mark for deletion
				sliceNamesToDelete.Insert(existingSlice.Name)
			} else {
				// 新 的 旧 的 都 有 值 ，发 生 更 新  了 ， 直 接 用 新 的 覆 盖 旧 的
				// otherwise, copy and mark for update
				epSlice := existingSlice.DeepCopy()
				epSlice.Endpoints = newEndpoints
				epSlice.Labels = labels
				slicesByName[existingSlice.Name] = epSlice
				sliceNamesToUpdate.Insert(epSlice.Name)
			}
		} else if labelsChanged { /* 没 有 发 生 变 更 ， 但 是  labels 变 了*/
			// if labels have changed, copy and mark for update
			epSlice := existingSlice.DeepCopy()
			epSlice.Labels = labels
			slicesByName[existingSlice.Name] = epSlice
			sliceNamesToUpdate.Insert(epSlice.Name)
		} else {
			// 没 有 发 生 变 更
			// slices with no changes will be useful if there are leftover endpoints
			sliceNamesUnchanged.Insert(existingSlice.Name)
		}
	}
	// 剩下的就是新增的 endpoint
	numAdded := desiredSet.Len()

	// 2. If we still have desired endpoints to add and slices marked for update,
	//    iterate through the slices and fill them up with the desired endpoints.
	if desiredSet.Len() > 0 && sliceNamesToUpdate.Len() > 0 {
		slices := []*discovery.EndpointSlice{}
		for _, sliceName := range sliceNamesToUpdate.UnsortedList() {
			slices = append(slices, slicesByName[sliceName])
		}
		// Sort endpoint slices by length so we're filling up the fullest ones
		// first.
		sort.Sort(endpointSliceEndpointLen(slices))

		// Iterate through slices and fill them up with desired endpoints.
		for _, slice := range slices {
			// 给每一个 endpointSlice 都加上这个 endpoint
			for desiredSet.Len() > 0 && len(slice.Endpoints) < int(r.maxEndpointsPerSlice) {
				endpoint, _ := desiredSet.PopAny()
				slice.Endpoints = append(slice.Endpoints, *endpoint)
			}
		}
	}

	// 3. If there are still desired endpoints left at this point, we try to fit
	//    the endpoints in a single existing slice. If there are no slices with
	//    that capacity, we create new slices for the endpoints.
	//尝试填充到现有的 EndpointSlice 中：
	//遍历当前存在的 EndpointSlice，检查其剩余容量（空位）。
	//如果找到一个可以容纳这些端点的 Slice，就将剩余端点分配到这个 Slice 中。
	//创建新的 EndpointSlice：
	//如果没有任何现有的 Slice 能够容纳这些剩余端点，那么就 创建新的 EndpointSlice，并将这些端点加入其中。
	slicesToCreate := []*discovery.EndpointSlice{}

	for desiredSet.Len() > 0 {
		var sliceToFill *discovery.EndpointSlice

		// If the remaining amounts of endpoints is smaller than the max
		// endpoints per slice and we have slices that haven't already been
		// filled, try to fit them in one.
		if desiredSet.Len() < int(r.maxEndpointsPerSlice) && sliceNamesUnchanged.Len() > 0 {
			unchangedSlices := []*discovery.EndpointSlice{}
			for _, sliceName := range sliceNamesUnchanged.UnsortedList() {
				unchangedSlices = append(unchangedSlices, slicesByName[sliceName])
			}
			sliceToFill = getSliceToFill(unchangedSlices, desiredSet.Len(), int(r.maxEndpointsPerSlice))
		}

		// If we didn't find a sliceToFill, generate a new empty one.
		if sliceToFill == nil {
			sliceToFill = newEndpointSlice(logger, service, endpointMeta, r.controllerName)
		} else {
			// deep copy required to modify this slice.
			sliceToFill = sliceToFill.DeepCopy()
			slicesByName[sliceToFill.Name] = sliceToFill
		}

		// Fill the slice up with remaining endpoints.
		for desiredSet.Len() > 0 && len(sliceToFill.Endpoints) < int(r.maxEndpointsPerSlice) {
			endpoint, _ := desiredSet.PopAny()
			sliceToFill.Endpoints = append(sliceToFill.Endpoints, *endpoint)
		}

		// New slices will not have a Name set, use this to determine whether
		// this should be an update or create.
		if sliceToFill.Name != "" {
			sliceNamesToUpdate.Insert(sliceToFill.Name)
			sliceNamesUnchanged.Delete(sliceToFill.Name)
		} else {
			slicesToCreate = append(slicesToCreate, sliceToFill)
		}
	}

	// Build slicesToUpdate from slice names.
	slicesToUpdate := []*discovery.EndpointSlice{}
	for _, sliceName := range sliceNamesToUpdate.UnsortedList() {
		slicesToUpdate = append(slicesToUpdate, slicesByName[sliceName])
	}

	// Build slicesToDelete from slice names.
	slicesToDelete := []*discovery.EndpointSlice{}
	for _, sliceName := range sliceNamesToDelete.UnsortedList() {
		slicesToDelete = append(slicesToDelete, slicesByName[sliceName])
	}

	return slicesToCreate, slicesToUpdate, slicesToDelete, numAdded, numRemoved
}

func (r *Reconciler) DeleteService(namespace, name string) {
	r.metricsCache.DeleteService(types.NamespacedName{Namespace: namespace, Name: name})
}

func (r *Reconciler) GetControllerName() string {
	return r.controllerName
}

// ManagedByChanged returns true if one of the provided EndpointSlices is
// managed by the EndpointSlice controller while the other is not.
func (r *Reconciler) ManagedByChanged(endpointSlice1, endpointSlice2 *discovery.EndpointSlice) bool {
	return r.ManagedByController(endpointSlice1) != r.ManagedByController(endpointSlice2)
}

// ManagedByController returns true if the controller of the provided
// EndpointSlices is the EndpointSlice controller.
// 要检查这个 endpointSlice 是不是 被 endpointSliceController 管理的，因为别的第三方服务也可能会建立自己的 endpointSlices
func (r *Reconciler) ManagedByController(endpointSlice *discovery.EndpointSlice) bool {
	managedBy := endpointSlice.Labels[discovery.LabelManagedBy]
	return managedBy == r.controllerName
}
