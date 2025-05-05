/*
Copyright 2020 The Kubernetes Authors.

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

package server

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	apimachineryversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"
)

// resourceExpirationEvaluator holds info for deciding if a particular rest.Storage needs to excluded from the API
type resourceExpirationEvaluator struct {
	currentVersion *apimachineryversion.Version
	isAlpha        bool
	// This is usually set for testing for which tests need to be removed.  This prevent insta-failing CI.
	// Set KUBE_APISERVER_STRICT_REMOVED_API_HANDLING_IN_ALPHA to see what will be removed when we tag beta
	strictRemovedHandlingInAlpha bool
	// This is usually set by a cluster-admin looking for a short-term escape hatch after something bad happened.
	// This should be made a flag before merge
	// Set KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE to prevent removing APIs for one more release.
	serveRemovedAPIsOneMoreRelease bool
}

// ResourceExpirationEvaluator indicates whether or not a resource should be served.
type ResourceExpirationEvaluator interface {
	// RemoveDeletedKinds inspects the storage map and modifies it in place by removing storage for kinds that have been deleted.
	// versionedResourcesStorageMap mirrors the field on APIGroupInfo, it's a map from version to resource to the storage.
	RemoveDeletedKinds(groupName string, versioner runtime.ObjectVersioner, versionedResourcesStorageMap map[string]map[string]rest.Storage)
	// ShouldServeForVersion returns true if a particular version cut off is after the current version
	ShouldServeForVersion(majorRemoved, minorRemoved int) bool
}

// 检查传入的 currentVersion（当前 Kubernetes 版本）是否为 nil，如果是，则返回错误。
func NewResourceExpirationEvaluator(currentVersion *apimachineryversion.Version) (ResourceExpirationEvaluator, error) {
	if currentVersion == nil {
		return nil, fmt.Errorf("empty NewResourceExpirationEvaluator currentVersion")
	}
	klog.V(1).Infof("NewResourceExpirationEvaluator with currentVersion: %s.", currentVersion)
	ret := &resourceExpirationEvaluator{
		strictRemovedHandlingInAlpha: false,
	}
	// Only keeps the major and minor versions from input version.
	//从 currentVersion 中提取 Major（主版本号，如 1）和 Minor（次版本号，如 22），并组合成 MajorMinor 格式（如 1.22），存储到 ret.currentVersion。
	ret.currentVersion = apimachineryversion.MajorMinor(currentVersion.Major(), currentVersion.Minor())
	//检查 currentVersion 的 PreRelease 字段是否包含 "alpha"，如果是，则 ret.isAlpha = true，表示当前运行的是 Alpha 版本的 Kubernetes。
	ret.isAlpha = strings.Contains(currentVersion.PreRelease(), "alpha")
	//控制是否在 Alpha 版本中严格处理已移除的 API（如果设为 true，则已移除的 API 会直接拒绝请求，而不是继续兼容）。
	if envString, ok := os.LookupEnv("KUBE_APISERVER_STRICT_REMOVED_API_HANDLING_IN_ALPHA"); !ok {
		// do nothing
	} else if envBool, err := strconv.ParseBool(envString); err != nil {
		return nil, err
	} else {
		ret.strictRemovedHandlingInAlpha = envBool
	}
	//控制是否在 API 被正式移除后，仍然多保留一个版本（如果设为 true，则即使 API 已标记为移除，仍然会继续提供服务一段时间）
	if envString, ok := os.LookupEnv("KUBE_APISERVER_SERVE_REMOVED_APIS_FOR_ONE_RELEASE"); !ok {
		// do nothing
	} else if envBool, err := strconv.ParseBool(envString); err != nil {
		return nil, err
	} else {
		ret.serveRemovedAPIsOneMoreRelease = envBool
	}

	return ret, nil
}

func (e *resourceExpirationEvaluator) shouldServe(gv schema.GroupVersion, versioner runtime.ObjectVersioner, resourceServingInfo rest.Storage) bool {
	internalPtr := resourceServingInfo.New()

	target := gv
	// honor storage that overrides group version (used for things like scale subresources)
	if versionProvider, ok := resourceServingInfo.(rest.GroupVersionKindProvider); ok {
		target = versionProvider.GroupVersionKind(target).GroupVersion()
	}

	versionedPtr, err := versioner.ConvertToVersion(internalPtr, target)
	if err != nil {
		utilruntime.HandleError(err)
		return false
	}

	introduced, ok := versionedPtr.(introducedInterface)
	if ok {
		majorIntroduced, minorIntroduced := introduced.APILifecycleIntroduced()
		verIntroduced := apimachineryversion.MajorMinor(uint(majorIntroduced), uint(minorIntroduced))
		if e.currentVersion.LessThan(verIntroduced) {
			return false
		}
	}

	removed, ok := versionedPtr.(removedInterface)
	if !ok {
		return true
	}
	majorRemoved, minorRemoved := removed.APILifecycleRemoved()
	return e.ShouldServeForVersion(majorRemoved, minorRemoved)
}

func (e *resourceExpirationEvaluator) ShouldServeForVersion(majorRemoved, minorRemoved int) bool {
	removedVer := apimachineryversion.MajorMinor(uint(majorRemoved), uint(minorRemoved))
	if removedVer.GreaterThan(e.currentVersion) {
		return true
	}
	if removedVer.LessThan(e.currentVersion) {
		return false
	}
	// at this point major and minor are equal, so this API should be removed when the current release GAs.
	// If this is an alpha tag, don't remove by default, but allow the option.
	// If the cluster-admin has requested serving one more release, allow it.
	if e.isAlpha && e.strictRemovedHandlingInAlpha { // don't serve in alpha if we want strict handling
		return false
	}
	if e.isAlpha { // alphas are allowed to continue serving expired betas while we clean up the test
		return true
	}
	if e.serveRemovedAPIsOneMoreRelease { // cluster-admins are allowed to kick the can one release down the road
		return true
	}
	return false
}

type removedInterface interface {
	APILifecycleRemoved() (major, minor int)
}

// Object interface generated from "k8s:prerelease-lifecycle-gen:introduced" tags in types.go.
type introducedInterface interface {
	APILifecycleIntroduced() (major, minor int)
}

// removeDeletedKinds inspects the storage map and modifies it in place by removing storage for kinds that have been deleted.
// versionedResourcesStorageMap mirrors the field on APIGroupInfo, it's a map from version to resource to the storage.
func (e *resourceExpirationEvaluator) RemoveDeletedKinds(groupName string, versioner runtime.ObjectVersioner, versionedResourcesStorageMap map[string]map[string]rest.Storage) {
	versionsToRemove := sets.NewString()
	for apiVersion := range sets.StringKeySet(versionedResourcesStorageMap) {
		versionToResource := versionedResourcesStorageMap[apiVersion]
		resourcesToRemove := sets.NewString()
		for resourceName, resourceServingInfo := range versionToResource {
			if !e.shouldServe(schema.GroupVersion{Group: groupName, Version: apiVersion}, versioner, resourceServingInfo) {
				resourcesToRemove.Insert(resourceName)
			}
		}

		for resourceName := range versionedResourcesStorageMap[apiVersion] {
			if !shouldRemoveResourceAndSubresources(resourcesToRemove, resourceName) {
				continue
			}

			klog.V(1).Infof("Removing resource %v.%v.%v because it is time to stop serving it per APILifecycle.", resourceName, apiVersion, groupName)
			storage := versionToResource[resourceName]
			storage.Destroy()
			delete(versionToResource, resourceName)
		}
		versionedResourcesStorageMap[apiVersion] = versionToResource

		if len(versionedResourcesStorageMap[apiVersion]) == 0 {
			versionsToRemove.Insert(apiVersion)
		}
	}

	for _, apiVersion := range versionsToRemove.List() {
		klog.V(1).Infof("Removing version %v.%v because it is time to stop serving it because it has no resources per APILifecycle.", apiVersion, groupName)
		delete(versionedResourcesStorageMap, apiVersion)
	}
}

func shouldRemoveResourceAndSubresources(resourcesToRemove sets.String, resourceName string) bool {
	for _, resourceToRemove := range resourcesToRemove.List() {
		if resourceName == resourceToRemove {
			return true
		}
		// our API works on nesting, so you can have deployments, deployments/status, and deployments/scale.  Not all subresources
		// serve the parent type, but if the parent type (deployments in this case), has been removed, it's subresources should be removed too.
		if strings.HasPrefix(resourceName, resourceToRemove+"/") {
			return true
		}
	}
	return false
}
