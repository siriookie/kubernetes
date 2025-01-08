/*
Copyright 2016 The Kubernetes Authors.

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

// Package app implements a server that runs a set of active
// components.  This includes replication controllers, service endpoints and
// nodes.
package app

import (
	"context"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/scale"
	"k8s.io/controller-manager/controller"
	"k8s.io/kubernetes/cmd/kube-controller-manager/names"
	"k8s.io/kubernetes/pkg/controller/disruption"
)

// PDB 主要用于维护操作期间的可用性控制，防止过多的 Pod 同时不可用。
// 单独创建的 PodDisruptionBudget (PDB) 和在 Deployment 中定义的 maxUnavailable 是两个不同的概念，它们之间并没有直接关联。以下是它们之间的区别：
//
// PodDisruptionBudget (PDB):
// PDB 是一个资源，用于限制在维护操作（如节点升级或手动干预）期间，可以同时中断的 Pod 数量。它确保在 Pod 被删除或迁移时，仍然有一定数量的 Pod 处于可用状态。
// 例如，minAvailable: 2 表示在任何时间点至少要有 2 个 Pod 可用。
// Deployment 中的 maxUnavailable:
// maxUnavailable 是 Deployment 的一个字段，用于指定在滚动更新过程中可以不可用的 Pod 的最大数量或百分比。这是关于更新期间的可用性控制。
// 例如，maxUnavailable: 1 表示在更新过程中，最多可以有 1 个 Pod 同时不可用。
func newDisruptionControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.DisruptionController,
		aliases:  []string{"disruption"},
		initFunc: startDisruptionController,
	}
}

func startDisruptionController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	client := controllerContext.ClientBuilder.ClientOrDie("disruption-controller")
	config := controllerContext.ClientBuilder.ConfigOrDie("disruption-controller")
	scaleKindResolver := scale.NewDiscoveryScaleKindResolver(client.Discovery())
	scaleClient, err := scale.NewForConfig(config, controllerContext.RESTMapper, dynamic.LegacyAPIPathResolverFunc, scaleKindResolver)
	if err != nil {
		return nil, false, err
	}

	go disruption.NewDisruptionController(
		ctx,
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Policy().V1().PodDisruptionBudgets(),
		controllerContext.InformerFactory.Core().V1().ReplicationControllers(),
		controllerContext.InformerFactory.Apps().V1().ReplicaSets(),
		controllerContext.InformerFactory.Apps().V1().Deployments(),
		controllerContext.InformerFactory.Apps().V1().StatefulSets(),
		client,
		controllerContext.RESTMapper,
		scaleClient,
		client.Discovery(),
	).Run(ctx)
	return nil, true, nil
}
