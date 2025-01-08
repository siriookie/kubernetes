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

package topologycache

import (
	"fmt"
	"math"
	"sync"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

const (
	// overloadThreshold represents the maximum overload any individual endpoint
	// should be exposed to.
	overloadThreshold float64 = 0.2
)

// TopologyCache tracks the distribution of Nodes and endpoints across zones.
type TopologyCache struct {
	lock                    sync.Mutex
	sufficientNodeInfo      bool
	cpuByZone               map[string]*resource.Quantity
	cpuRatiosByZone         map[string]float64
	endpointsByService      map[string]map[discovery.AddressType]EndpointZoneInfo
	hintsPopulatedByService sets.Set[string]
}

// EndpointZoneInfo tracks the distribution of endpoints across zones for a
// Service.
type EndpointZoneInfo map[string]int

// allocation describes the number of endpoints that should be allocated for a
// zone.
type allocation struct {
	minimum int
	maximum int
	desired float64
}

// NewTopologyCache initializes a new TopologyCache.
func NewTopologyCache() *TopologyCache {
	return &TopologyCache{
		cpuByZone:               map[string]*resource.Quantity{},
		cpuRatiosByZone:         map[string]float64{},
		endpointsByService:      map[string]map[discovery.AddressType]EndpointZoneInfo{},
		hintsPopulatedByService: sets.Set[string]{},
	}
}

// GetOverloadedServices returns a list of Service keys that refer to Services
// that have crossed the overload threshold for any zone.
func (t *TopologyCache) GetOverloadedServices() []string {
	t.lock.Lock()
	defer t.lock.Unlock()

	svcKeys := []string{}
	for svcKey, eziByAddrType := range t.endpointsByService {
		for _, ezi := range eziByAddrType {
			if serviceOverloaded(ezi, t.cpuRatiosByZone) {
				svcKeys = append(svcKeys, svcKey)
				break
			}
		}
	}

	return svcKeys
}

// AddHints adds or updates topology hints on EndpointSlices and returns updated
// lists of EndpointSlices to create and update.
// AddHints 函数用于为 EndpointSlices 添加或更新拓扑提示，并返回需要创建和更新的 EndpointSlices 列表。
func (t *TopologyCache) AddHints(logger klog.Logger, si *SliceInfo) ([]*discovery.EndpointSlice, []*discovery.EndpointSlice, []*EventBuilder) {
	// 所有 endpoints
	totalEndpoints := si.getTotalReadyEndpoints()
	allocations, allocationsEvent := t.getAllocations(totalEndpoints)
	events := []*EventBuilder{}
	if allocationsEvent != nil {
		logger.Info(allocationsEvent.Message+", removing hints", "key", si.ServiceKey, "addressType", si.AddressType)
		allocationsEvent.Message = FormatWithAddressType(allocationsEvent.Message, si.AddressType)
		events = append(events, allocationsEvent)
		t.RemoveHints(si.ServiceKey, si.AddressType)
		slicesToCreate, slicesToUpdate := RemoveHintsFromSlices(si)
		return slicesToCreate, slicesToUpdate, events
	}

	allocatedHintsByZone := si.getAllocatedHintsByZone(allocations)

	allocatableSlices := si.ToCreate
	for _, slice := range si.ToUpdate {
		allocatableSlices = append(allocatableSlices, slice)
	}

	// step 1: assign same-zone hints for all endpoints as a starting point.
	for _, slice := range allocatableSlices {
		for i, endpoint := range slice.Endpoints {
			if !EndpointReady(endpoint) {
				endpoint.Hints = nil
				continue
			}
			if endpoint.Zone == nil || *endpoint.Zone == "" {
				logger.Info("Endpoint found without zone specified, removing hints", "key", si.ServiceKey, "addressType", si.AddressType)
				events = append(events, &EventBuilder{
					EventType: v1.EventTypeWarning,
					Reason:    "TopologyAwareHintsDisabled",
					Message:   FormatWithAddressType(NoZoneSpecified, si.AddressType),
				})
				t.RemoveHints(si.ServiceKey, si.AddressType)
				slicesToCreate, slicesToUpdate := RemoveHintsFromSlices(si)
				return slicesToCreate, slicesToUpdate, events
			}

			allocatedHintsByZone[*endpoint.Zone]++
			slice.Endpoints[i].Hints = &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: *endpoint.Zone}}}
		}
	}

	// step 2. Identify which zones need to donate slices and which need more.
	givingZones, receivingZones := getGivingAndReceivingZones(allocations, allocatedHintsByZone)

	// step 3. Redistribute endpoints based on data from step 2.
	redistributions := redistributeHints(logger, allocatableSlices, givingZones, receivingZones)

	for zone, diff := range redistributions {
		allocatedHintsByZone[zone] += diff
	}

	if len(allocatedHintsByZone) == 0 {
		logger.V(2).Info("No hints allocated for zones, removing them", "key", si.ServiceKey, "addressType", si.AddressType)
		events = append(events, &EventBuilder{
			EventType: v1.EventTypeWarning,
			Reason:    "TopologyAwareHintsDisabled",
			Message:   FormatWithAddressType(NoAllocatedHintsForZones, si.AddressType),
		})
		t.RemoveHints(si.ServiceKey, si.AddressType)
		slicesToCreate, slicesToUpdate := RemoveHintsFromSlices(si)
		return slicesToCreate, slicesToUpdate, events
	}

	t.lock.Lock()
	defer t.lock.Unlock()
	hintsEnabled := t.hasPopulatedHintsLocked(si.ServiceKey)
	t.setHintsLocked(si.ServiceKey, si.AddressType, allocatedHintsByZone)

	// if hints were not enabled before, we publish an event to indicate we enabled them.
	if !hintsEnabled {
		logger.Info("Topology Aware Hints has been enabled, adding hints.", "key", si.ServiceKey, "addressType", si.AddressType)
		events = append(events, &EventBuilder{
			EventType: v1.EventTypeNormal,
			Reason:    "TopologyAwareHintsEnabled",
			Message:   FormatWithAddressType(TopologyAwareHintsEnabled, si.AddressType),
		})
	}
	return si.ToCreate, si.ToUpdate, events
}

// SetHints sets topology hints for the provided serviceKey and addrType in this
// cache.
func (t *TopologyCache) SetHints(serviceKey string, addrType discovery.AddressType, allocatedHintsByZone EndpointZoneInfo) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.setHintsLocked(serviceKey, addrType, allocatedHintsByZone)
}

func (t *TopologyCache) setHintsLocked(serviceKey string, addrType discovery.AddressType, allocatedHintsByZone EndpointZoneInfo) {
	_, ok := t.endpointsByService[serviceKey]
	if !ok {
		t.endpointsByService[serviceKey] = map[discovery.AddressType]EndpointZoneInfo{}
	}
	t.endpointsByService[serviceKey][addrType] = allocatedHintsByZone

	t.hintsPopulatedByService.Insert(serviceKey)
}

// RemoveHints removes topology hints for the provided serviceKey and addrType
// from this cache.
func (t *TopologyCache) RemoveHints(serviceKey string, addrType discovery.AddressType) {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, ok := t.endpointsByService[serviceKey]
	if ok {
		delete(t.endpointsByService[serviceKey], addrType)
	}
	if len(t.endpointsByService[serviceKey]) == 0 {
		delete(t.endpointsByService, serviceKey)
	}
	t.hintsPopulatedByService.Delete(serviceKey)
}

// SetNodes updates the Node distribution for the TopologyCache.
// 节点过滤：
//
// 遍历传入的 nodes 列表，逐个检查节点。每个节点首先会进行标签筛选，若标签符合排除条件或节点没有准备好（即不是 ready 状态），则跳过该节点。
// 获取节点的 CPU 和区域信息：
//
// 从每个节点中获取其分配的 CPU 数量和区域（zone）标签。若节点缺少区域标签或者 CPU 信息为空，则认为该节点信息不足，将导致整个流程失败。
// 更新节点的 CPU 和区域分布信息：
//
// 如果节点的信息有效，累加各个区域（zone）的 CPU 信息并统计所有节点的总 CPU 数量（totalCPU）。
// 如果某个区域第一次出现，则为其创建一个 CPU 条目。如果该区域已经存在，则累加该区域的 CPU 总数。
// 拓扑缓存的更新：
//
// 如果所有节点的 CPU 总和为零，或者某些节点信息不足（例如缺少区域或 CPU 信息），或者有效区域数少于 2 个，则记录为信息不足，并清空相关的 CPU 数据。
// 如果节点信息充足，计算每个区域的 CPU 比例，更新 cpuRatiosByZone（区域 CPU 比例）。
func (t *TopologyCache) SetNodes(logger klog.Logger, nodes []*v1.Node) {
	cpuByZone := map[string]*resource.Quantity{}
	sufficientNodeInfo := true

	totalCPU := resource.Quantity{}

	for _, node := range nodes {
		if hasExcludedLabels(node.Labels) {
			logger.V(2).Info("Ignoring node because it has an excluded label", "node", klog.KObj(node))
			continue
		}
		if !isNodeReady(node) {
			logger.V(2).Info("Ignoring node because it is not ready", "node", klog.KObj(node))
			continue
		}

		nodeCPU := node.Status.Allocatable.Cpu()
		zone, ok := node.Labels[v1.LabelTopologyZone]

		// TODO(robscott): Figure out if there's an acceptable proportion of
		// nodes with inadequate information. The current logic means that as
		// soon as we find any node without a zone or allocatable CPU specified,
		// we bail out entirely. Bailing out at this level will make our cluster
		// wide ratios nil, which would result in slices for all Services having
		// their hints removed.
		if !ok || zone == "" || nodeCPU.IsZero() {
			cpuByZone = map[string]*resource.Quantity{}
			sufficientNodeInfo = false
			logger.Info("Can't get CPU or zone information for node", "node", klog.KObj(node))
			break
		}

		totalCPU.Add(*nodeCPU)
		if _, ok = cpuByZone[zone]; !ok {
			cpuByZone[zone] = nodeCPU
		} else {
			cpuByZone[zone].Add(*nodeCPU)
		}
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	if totalCPU.IsZero() || !sufficientNodeInfo || len(cpuByZone) < 2 {
		logger.V(2).Info("Insufficient node info for topology hints", "totalZones", len(cpuByZone), "totalCPU", totalCPU.String(), "sufficientNodeInfo", sufficientNodeInfo)
		t.sufficientNodeInfo = false
		t.cpuByZone = nil
		t.cpuRatiosByZone = nil

	} else {
		t.sufficientNodeInfo = sufficientNodeInfo
		t.cpuByZone = cpuByZone

		t.cpuRatiosByZone = map[string]float64{}
		for zone, cpu := range cpuByZone {
			t.cpuRatiosByZone[zone] = float64(cpu.MilliValue()) / float64(totalCPU.MilliValue())
		}
	}
}

// HasPopulatedHints checks whether there are populated hints for a given service in the cache.
func (t *TopologyCache) HasPopulatedHints(serviceKey string) bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.hasPopulatedHintsLocked(serviceKey)
}

func (t *TopologyCache) hasPopulatedHintsLocked(serviceKey string) bool {
	return t.hintsPopulatedByService.Has(serviceKey)
}

// getAllocations returns a set of minimum and maximum allocations per zone. If
// it is not possible to provide allocations that are below the overload
// threshold, a nil value will be returned.
// 这段代码定义了 getAllocations 函数，它用于计算并返回每个区域的最小和最大分配。这些分配表示每个区域应该分配多少端点（即“numEndpoints”），
// 以保持负载平衡和满足资源需求。
//
// 具体功能包括：
//
// 锁定资源：使用 t.lock.Lock() 和 defer t.lock.Unlock() 来确保线程安全，以防多个协程访问 cpuRatiosByZone 数据时产生竞争条件。
// 检查 CPU 比例：首先检查 cpuRatiosByZone 是否为空，若为空，返回警告事件，表示拓扑感知提示功能已禁用。
// 检查区域数目：判断区域数是否少于 2（即少于两个区域的数据），若少，返回警告，表示只有一个区域的节点准备就绪，拓扑感知提示被禁用。
// 检查区域数与端点数：如果区域数大于端点数，则返回警告，表示缺少足够的端点来分配到每个区域。
// 分配端点：
// 对每个区域，根据 CPU 比例计算每个区域的期望端点数。
// 计算每个区域的最小分配值，并确保总的最小分配值不超过提供的端点总数。
// 如果某个区域的最小分配值超过总端点数，返回警告事件。
// 最大分配：最终计算每个区域的最大分配数量，它是最小分配量加上剩余的端点数。
func (t *TopologyCache) getAllocations(numEndpoints int) (map[string]allocation, *EventBuilder) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// it is similar to checking !t.sufficientNodeInfo
	if t.cpuRatiosByZone == nil {
		return nil, &EventBuilder{
			EventType: v1.EventTypeWarning,
			Reason:    "TopologyAwareHintsDisabled",
			Message:   InsufficientNodeInfo,
		}
	}
	if len(t.cpuRatiosByZone) < 2 {
		return nil, &EventBuilder{
			EventType: v1.EventTypeWarning,
			Reason:    "TopologyAwareHintsDisabled",
			Message:   NodesReadyInOneZoneOnly,
		}
	}
	if len(t.cpuRatiosByZone) > numEndpoints {
		return nil, &EventBuilder{
			EventType: v1.EventTypeWarning,
			Reason:    "TopologyAwareHintsDisabled",
			Message:   fmt.Sprintf("%s (%d endpoints, %d zones)", InsufficientNumberOfEndpoints, numEndpoints, len(t.cpuRatiosByZone)),
		}
	}

	remainingMinEndpoints := numEndpoints
	minTotal := 0
	allocations := map[string]allocation{}

	for zone, ratio := range t.cpuRatiosByZone {
		desired := ratio * float64(numEndpoints)
		minimum := int(math.Ceil(desired * (1 / (1 + overloadThreshold))))
		allocations[zone] = allocation{
			minimum: minimum,
			desired: math.Max(desired, float64(minimum)),
		}
		minTotal += minimum
		remainingMinEndpoints -= minimum
		if remainingMinEndpoints < 0 {
			return nil, &EventBuilder{
				EventType: v1.EventTypeWarning,
				Reason:    "TopologyAwareHintsDisabled",
				Message:   fmt.Sprintf("%s (%d endpoints, %d zones)", MinAllocationExceedsOverloadThreshold, numEndpoints, len(t.cpuRatiosByZone)),
			}
		}
	}

	for zone, allocation := range allocations {
		allocation.maximum = allocation.minimum + numEndpoints - minTotal
		allocations[zone] = allocation
	}

	return allocations, nil
}

// Nodes with any of these labels set to any value will be excluded from
// topology capacity calculations.
func hasExcludedLabels(labels map[string]string) bool {
	if len(labels) == 0 {
		return false
	}
	if _, ok := labels["node-role.kubernetes.io/control-plane"]; ok {
		return true
	}
	if _, ok := labels["node-role.kubernetes.io/master"]; ok {
		return true
	}
	return false
}
