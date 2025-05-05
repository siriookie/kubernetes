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

package scale

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/wait"
	scaleclient "k8s.io/client-go/scale"
)

// Scaler provides an interface for resources that can be scaled.
type Scaler interface {
	// Scale scales the named resource after checking preconditions. It optionally
	// retries in the event of resource version mismatch (if retry is not nil),
	// and optionally waits until the status of the resource matches newSize (if wait is not nil)
	// TODO: Make the implementation of this watch-based (#56075) once #31345 is fixed.
	Scale(namespace, name string, newSize uint, preconditions *ScalePrecondition, retry, wait *RetryParams, gvr schema.GroupVersionResource, dryRun bool) error
	// ScaleSimple does a simple one-shot attempt at scaling - not useful on its own, but
	// a necessary building block for Scale
	ScaleSimple(namespace, name string, preconditions *ScalePrecondition, newSize uint, gvr schema.GroupVersionResource, dryRun bool) (updatedResourceVersion string, err error)
}

// NewScaler get a scaler for a given resource
func NewScaler(scalesGetter scaleclient.ScalesGetter) Scaler {
	return &genericScaler{scalesGetter}
}

// ScalePrecondition describes a condition that must be true for the scale to take place
// If CurrentSize == -1, it is ignored.
// If CurrentResourceVersion is the empty string, it is ignored.
// Otherwise they must equal the values in the resource for it to be valid.
type ScalePrecondition struct {
	Size            int
	ResourceVersion string
}

// A PreconditionError is returned when a resource fails to match
// the scale preconditions passed to kubectl.
type PreconditionError struct {
	Precondition  string
	ExpectedValue string
	ActualValue   string
}

func (pe PreconditionError) Error() string {
	return fmt.Sprintf("Expected %s to be %s, was %s", pe.Precondition, pe.ExpectedValue, pe.ActualValue)
}

// RetryParams encapsulates the retry parameters used by kubectl's scaler.
type RetryParams struct {
	Interval, Timeout time.Duration
}

func NewRetryParams(interval, timeout time.Duration) *RetryParams {
	return &RetryParams{interval, timeout}
}

// ScaleCondition is a closure around Scale that facilitates retries via util.wait
func ScaleCondition(r Scaler, // 资源缩放器
	precondition *ScalePrecondition, // 预检查条件
	namespace, name string, // 资源命名空间 & 资源名称
	count uint, // 目标副本数
	updatedResourceVersion *string, // 记录最新 ResourceVersion
	gvr schema.GroupVersionResource, // 资源的 GVR 标识
	dryRun bool, // 是否 DryRun
) wait.ConditionWithContextFunc {
	return func(context.Context) (bool, error) {
		rv, err := r.ScaleSimple(namespace, name, precondition, count, gvr, dryRun)
		if updatedResourceVersion != nil {
			//存储 ScaleSimple 返回的 ResourceVersion（更新后的版本号）。
			*updatedResourceVersion = rv
		}
		// Retry only on update conflicts.
		//"Conflict"（冲突）通常发生在并发修改资源时，如：
		//
		//A 进程获取了资源 deployment
		//
		//B 进程修改了 deployment
		//
		//A 进程尝试更新 deployment，但因为它的 ResourceVersion 过期，导致更新失败。
		if apierrors.IsConflict(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}
}

// validateGeneric ensures that the preconditions match. Returns nil if they are valid, otherwise an error
func (precondition *ScalePrecondition) validate(scale *autoscalingv1.Scale) error {
	if precondition.Size != -1 && int(scale.Spec.Replicas) != precondition.Size {
		return PreconditionError{"replicas", strconv.Itoa(precondition.Size), strconv.Itoa(int(scale.Spec.Replicas))}
	}
	if len(precondition.ResourceVersion) > 0 && scale.ResourceVersion != precondition.ResourceVersion {
		return PreconditionError{"resource version", precondition.ResourceVersion, scale.ResourceVersion}
	}
	return nil
}

// genericScaler can update scales for resources in a particular namespace
type genericScaler struct {
	scaleNamespacer scaleclient.ScalesGetter
}

var _ Scaler = &genericScaler{}

// ScaleSimple updates a scale of a given resource. It returns the resourceVersion of the scale if the update was successful.
// ScaleSimple 这个函数的作用是调整 Kubernetes 资源（如 Deployment、StatefulSet、ReplicaSet）的副本数，并返回更新后的 ResourceVersion
func (s *genericScaler) ScaleSimple(namespace, name string, // 资源所在的命名空间 和 资源名称
	preconditions *ScalePrecondition, // 预检查条件（可选）
	newSize uint, // 目标副本数
	gvr schema.GroupVersionResource, // 资源的 GroupVersionResource（标识资源类型）
	dryRun bool, // 是否为 DryRun（不真正执行，只模拟）
) (updatedResourceVersion string, err error) {
	if preconditions != nil {
		//先获取当前 scale 资源：
		scale, err := s.scaleNamespacer.Scales(namespace).Get(context.TODO(), gvr.GroupResource(), name, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		if err = preconditions.validate(scale); err != nil {
			return "", err
		}
		//更新 scale.Spec.Replicas：
		scale.Spec.Replicas = int32(newSize)
		updateOptions := metav1.UpdateOptions{}
		if dryRun {
			updateOptions.DryRun = []string{metav1.DryRunAll}
		}
		updatedScale, err := s.scaleNamespacer.Scales(namespace).Update(context.TODO(), gvr.GroupResource(), scale, updateOptions)
		if err != nil {
			return "", err
		}
		return updatedScale.ResourceVersion, nil
	}

	// objectForReplicas is used for encoding scale patch
	type objectForReplicas struct {
		Replicas uint `json:"replicas"`
	}
	// objectForSpec is used for encoding scale patch
	type objectForSpec struct {
		Spec objectForReplicas `json:"spec"`
	}
	spec := objectForSpec{
		Spec: objectForReplicas{Replicas: newSize},
	}
	patch, err := json.Marshal(&spec)
	if err != nil {
		return "", err
	}
	patchOptions := metav1.PatchOptions{}
	if dryRun {
		patchOptions.DryRun = []string{metav1.DryRunAll}
	}
	//使用 Patch 方法来更新 spec.replicas：
	//更轻量级，只修改 replicas 字段，不影响其他字段。
	updatedScale, err := s.scaleNamespacer.Scales(namespace).Patch(context.TODO(), gvr, name, types.MergePatchType, patch, patchOptions)
	if err != nil {
		return "", err
	}
	return updatedScale.ResourceVersion, nil
}

// Scale updates a scale of a given resource to a new size, with optional precondition check (if preconditions is not nil),
// optional retries (if retry is not nil), and then optionally waits for the status to reach desired count.
// 这个函数 Scale 是 Kubernetes 中的一个通用缩放（scaling）实现，用于调整某个资源（如 Deployment、ReplicaSet、StatefulSet 等）的副本数。
// 它支持预检查条件、重试机制，以及等待副本数达到期望值。
func (s *genericScaler) Scale(namespace, resourceName string, newSize uint, preconditions *ScalePrecondition, retry, waitForReplicas *RetryParams, gvr schema.GroupVersionResource, dryRun bool) error {
	if retry == nil {
		// make it try only once, immediately
		retry = &RetryParams{Interval: time.Millisecond, Timeout: time.Millisecond}
	}
	cond := ScaleCondition(s, preconditions, namespace, resourceName, newSize, nil, gvr, dryRun)
	if err := wait.PollUntilContextTimeout(context.Background(), retry.Interval, retry.Timeout, true, cond); err != nil {
		return err
	}
	if waitForReplicas != nil {
		return WaitForScaleHasDesiredReplicas(s.scaleNamespacer, gvr.GroupResource(), resourceName, namespace, newSize, waitForReplicas)
	}
	return nil
}

// scaleHasDesiredReplicas returns a condition that will be true if and only if the desired replica
// count for a scale (Spec) equals its updated replicas count (Status)
func scaleHasDesiredReplicas(sClient scaleclient.ScalesGetter, gr schema.GroupResource, resourceName string, namespace string, desiredReplicas int32) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (bool, error) {
		actualScale, err := sClient.Scales(namespace).Get(ctx, gr, resourceName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		// this means the desired scale target has been reset by something else
		if actualScale.Spec.Replicas != desiredReplicas {
			return true, nil
		}
		return actualScale.Spec.Replicas == actualScale.Status.Replicas &&
			desiredReplicas == actualScale.Status.Replicas, nil
	}
}

// WaitForScaleHasDesiredReplicas waits until condition scaleHasDesiredReplicas is satisfied
// or returns error when timeout happens
func WaitForScaleHasDesiredReplicas(sClient scaleclient.ScalesGetter, gr schema.GroupResource, resourceName string, namespace string, newSize uint, waitForReplicas *RetryParams) error {
	if waitForReplicas == nil {
		return fmt.Errorf("waitForReplicas parameter cannot be nil")
	}
	err := wait.PollUntilContextTimeout(context.Background(), waitForReplicas.Interval, waitForReplicas.Timeout, true, scaleHasDesiredReplicas(sClient, gr, resourceName, namespace, int32(newSize)))

	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("timed out waiting for %q to be synced", resourceName)
	}
	return err
}
