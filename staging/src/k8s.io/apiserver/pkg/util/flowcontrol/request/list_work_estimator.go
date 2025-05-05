/*
Copyright 2021 The Kubernetes Authors.

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

package request

import (
	"math"
	"net/http"
	"net/url"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	etcdfeature "k8s.io/apiserver/pkg/storage/feature"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
)

func newListWorkEstimator(countFn objectCountGetterFunc, config *WorkEstimatorConfig, maxSeatsFn maxSeatsFunc) WorkEstimatorFunc {
	estimator := &listWorkEstimator{
		config:        config,
		countGetterFn: countFn,
		maxSeatsFn:    maxSeatsFn,
	}
	return estimator.estimate
}

type listWorkEstimator struct {
	config        *WorkEstimatorConfig
	countGetterFn objectCountGetterFunc
	maxSeatsFn    maxSeatsFunc
}

func (e *listWorkEstimator) estimate(r *http.Request, flowSchemaName, priorityLevelName string) WorkEstimate {
	//设置本次估算允许的最小和最大 seat
	minSeats := e.config.MinimumSeats
	maxSeats := e.maxSeatsFn(priorityLevelName)
	if maxSeats == 0 || maxSeats > e.config.MaximumSeatsLimit {
		maxSeats = e.config.MaximumSeatsLimit
	}

	requestInfo, ok := apirequest.RequestInfoFrom(r.Context())
	//如果没有 RequestInfo，就返回最大 seat（兜底保护逻辑）。
	if !ok {
		// no RequestInfo should never happen, but to be on the safe side
		// let's return maximumSeats
		return WorkEstimate{InitialSeats: maxSeats}
	}
	// 判断请求是否是 get 类 list（即带有 metadata.name）
	//如果 list 请求带了 metadata.name 参数，通常表示它是伪装成 list 的 get 请求，例如：
	///api/v1/namespaces/default/pods?fieldSelector=metadata.name=foo
	//这种场景 seat 设置为最小（轻量操作）。
	if requestInfo.Name != "" {
		// Requests with metadata.name specified are usually executed as get
		// requests in storage layer so their width should be 1.
		// Example of such list requests:
		// /apis/certificates.k8s.io/v1/certificatesigningrequests?fieldSelector=metadata.name%3Dcsr-xxs4m
		// /api/v1/namespaces/test/configmaps?fieldSelector=metadata.name%3Dbig-deployment-1&limit=500&resourceVersion=0
		return WorkEstimate{InitialSeats: minSeats}
	}

	query := r.URL.Query()
	//将查询参数转换为标准 ListOptions 结构体，方便后续计算。
	//如果解析失败，直接返回最大 seats。
	listOptions := metav1.ListOptions{}
	if err := metav1.Convert_url_Values_To_v1_ListOptions(&query, &listOptions, nil); err != nil {
		klog.ErrorS(err, "Failed to convert options while estimating work for the list request")

		// This request is destined to fail in the validation layer,
		// return maximumSeats for this request to be consistent.
		return WorkEstimate{InitialSeats: maxSeats}
	}

	// For watch requests, we want to adjust the cost only if they explicitly request
	// sending initial events.
	// 对于 watch 请求：如果不需要发初始事件，返回 minSeats
	if requestInfo.Verb == "watch" {
		if listOptions.SendInitialEvents == nil || !*listOptions.SendInitialEvents {
			return WorkEstimate{InitialSeats: e.config.MinimumSeats}
		}
	}

	isListFromCache := requestInfo.Verb == "watch" || !shouldListFromStorage(query, &listOptions)

	numStored, err := e.countGetterFn(key(requestInfo))
	switch {
	case err == ObjectCountStaleErr:
		// object count going stale is indicative of degradation, so we should
		// be conservative here and allocate maximum seats to this list request.
		// NOTE: if a CRD is removed, its count will go stale first and then the
		// pruner will eventually remove the CRD from the cache.
		// 对象计数过时表明性能下降，因此我们应该在这里采取保守策略，为这个列表请求分配最大座位数。
		// 注意：如果一个CRD被移除，它的计数会首先过时，然后修剪器最终会从缓存中移除该CRD。
		return WorkEstimate{InitialSeats: maxSeats}
	case err == ObjectCountNotFoundErr:
		// there are multiple scenarios in which we can see this error:
		//  a. the type is truly unknown, a typo on the caller's part.
		//  b. the count has gone stale for too long and the pruner
		//     has removed the type from the cache.
		//  c. the type is an aggregated resource that is served by a
		//     different apiserver (thus its object count is not updated)
		// we don't have a way to distinguish between those situations.
		// However, in case c, the request is delegated to a different apiserver,
		// and thus its cost for our server is minimal. To avoid the situation
		// when aggregated API calls are overestimated, we allocate the minimum
		// possible seats (see #109106 as an example when being more conservative
		// led to problems).
		return WorkEstimate{InitialSeats: minSeats}
	case err != nil:
		// we should never be here since Get returns either ObjectCountStaleErr or
		// ObjectCountNotFoundErr, return maximumSeats to be on the safe side.
		klog.ErrorS(err, "Unexpected error from object count tracker")
		return WorkEstimate{InitialSeats: maxSeats}
	}

	limit := numStored
	if listOptions.Limit > 0 && listOptions.Limit < numStored {
		limit = listOptions.Limit
	}

	var estimatedObjectsToBeProcessed int64

	switch {
	case isListFromCache:
		//🟢 Case 1：来自缓存（watch cache）
		//isListFromCache == true 表示此次请求从 本地缓存（watch cache） 中读取。
		//处理的对象数量就等于缓存中记录的资源数：numStored。
		// TODO: For resources that implement indexes at the watchcache level,
		//  we need to adjust the cost accordingly
		estimatedObjectsToBeProcessed = numStored
	case listOptions.FieldSelector != "" || listOptions.LabelSelector != "":
		//🟡 Case 2：带字段或标签过滤器（FieldSelector / LabelSelector）
		//意味着这个 List 请求只关心部分资源，需要额外的 过滤计算。
		//所以它的处理开销更大。
		//保守估计：我们假设要 扫描全部 numStored 对象，但最多返回 limit 个 → 所以总处理量是 numStored + limit。
		estimatedObjectsToBeProcessed = numStored + limit
	default:
		//🔵 Case 3：普通 List 请求，没有 selector
		//通常这类请求就是获取前 limit 个对象。
		//但由于底层可能仍然扫描、处理部分额外对象（比如做分页、排序等），所以估算为 2 * limit。
		estimatedObjectsToBeProcessed = 2 * limit
	}

	// for now, our rough estimate is to allocate one seat to each 100 obejcts that
	// will be processed by the list request.
	// we will come up with a different formula for the transformation function and/or
	// fine tune this number in future iteratons.
	seats := uint64(math.Ceil(float64(estimatedObjectsToBeProcessed) / e.config.ObjectsPerSeat))

	// make sure we never return a seat of zero
	if seats < minSeats {
		seats = minSeats
	}
	if seats > maxSeats {
		seats = maxSeats
	}
	return WorkEstimate{InitialSeats: seats}
}

func key(requestInfo *apirequest.RequestInfo) string {
	groupResource := &schema.GroupResource{
		Group:    requestInfo.APIGroup,
		Resource: requestInfo.Resource,
	}
	return groupResource.String()
}

// NOTICE: Keep in sync with shouldDelegateList function in
//
//	staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go
func shouldListFromStorage(query url.Values, opts *metav1.ListOptions) bool {
	resourceVersion := opts.ResourceVersion
	match := opts.ResourceVersionMatch
	// 是一个 feature gate，控制是否允许从 watch cache 提供一致性较强的 List。
	consistentListFromCacheEnabled := utilfeature.DefaultFeatureGate.Enabled(features.ConsistentListFromCache)
	//RequestWatchProgress 是 etcd 的特性，是否支持 watch 进度反馈。
	requestWatchProgressSupported := etcdfeature.DefaultFeatureSupportChecker.Supports(storage.RequestWatchProgress)
	//如果用户没有指定 ResourceVersion 且不支持 ConsistentListFromCache，必须从 etcd 读取。
	// Serve consistent reads from storage if ConsistentListFromCache is disabled
	consistentReadFromStorage := resourceVersion == "" && !(consistentListFromCacheEnabled && requestWatchProgressSupported)
	// Watch cache doesn't support continuations, so serve them from etcd.
	//分页 list（带 continue 参数）不能从 cache 获取，只能从 etcd 获取，因为 cache 没有分页机制。
	hasContinuation := len(opts.Continue) > 0
	// Watch cache only supports ResourceVersionMatchNotOlderThan (default).
	// see https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-get-and-list
	//默认匹配模式是 NotOlderThan，这是 cache 能支持的唯一模式。
	//如果客户端请求了精确匹配等模式（Exact），或者用了 "legacy exact match"（老旧写法），就不能走 cache。
	isLegacyExactMatch := opts.Limit > 0 && match == "" && len(resourceVersion) > 0 && resourceVersion != "0"
	unsupportedMatch := match != "" && match != metav1.ResourceVersionMatchNotOlderThan || isLegacyExactMatch

	return consistentReadFromStorage || hasContinuation || unsupportedMatch
}
