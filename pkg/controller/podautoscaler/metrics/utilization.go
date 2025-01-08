/*
Copyright 2015 The Kubernetes Authors.

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

package metrics

import (
	"fmt"
)

// GetResourceUtilizationRatio takes in a set of metrics, a set of matching requests,
// and a target utilization percentage, and calculates the ratio of
// desired to actual utilization (returning that, the actual utilization, and the raw average value)
// GetResourceUtilizationRatio 接收一组metrics、一组request和一个目标利用率百分比（targetUtilization），
// 计算期望利用率与实际利用率的比率（同时返回该比率、实际利用率以及原始平均值）。
func GetResourceUtilizationRatio(metrics PodMetricsInfo, requests map[string]int64, targetUtilization int32) (utilizationRatio float64, currentUtilization int32, rawAverageValue int64, err error) {
	metricsTotal := int64(0)
	requestsTotal := int64(0)
	numEntries := 0
	// 计算所有pod 的metrics和requests 的 resource 总和
	for podName, metric := range metrics {
		request, hasRequest := requests[podName]
		if !hasRequest {
			// we check for missing requests elsewhere, so assuming missing requests == extraneous metrics
			continue
		}

		metricsTotal += metric.Value
		requestsTotal += request
		numEntries++
	}

	// if the set of requests is completely disjoint from the set of metrics,
	// then we could have an issue where the requests total is zero
	if requestsTotal == 0 {
		return 0, 0, 0, fmt.Errorf("no metrics returned matched known pods")
	}

	currentUtilization = int32((metricsTotal * 100) / requestsTotal)

	return float64(currentUtilization) / float64(targetUtilization), currentUtilization, metricsTotal / int64(numEntries), nil
}

// GetMetricUsageRatio takes in a set of metrics and a target usage value,
// and calculates the ratio of desired to actual usage
// (returning that and the actual usage)
// 收集多个 Pod 的当前指标值（比如 CPU 使用量）。
// 计算每个 Pod 的平均使用量。
// 与目标使用量比较，得出比率。
func GetMetricUsageRatio(metrics PodMetricsInfo, targetUsage int64) (usageRatio float64, currentUsage int64) {
	metricsTotal := int64(0)
	for _, metric := range metrics {
		metricsTotal += metric.Value
	}

	currentUsage = metricsTotal / int64(len(metrics))

	return float64(currentUsage) / float64(targetUsage), currentUsage
}
