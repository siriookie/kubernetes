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
	"fmt"
	"time"

	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/controller-manager/controller"
	"k8s.io/kubernetes/cmd/kube-controller-manager/names"
	"k8s.io/kubernetes/pkg/controller/daemon"
	"k8s.io/kubernetes/pkg/controller/deployment"
	"k8s.io/kubernetes/pkg/controller/replicaset"
	"k8s.io/kubernetes/pkg/controller/statefulset"
)

// 主要职责是监听daemon set、pod、node、controller version的变更
// 然后去给 daemon set在每个node 上创建它应该存在的 pod 数
// 步骤：先保证每个 node 上都存在pod，然后保证 pod 的数量是符合设置的
// 再 rolling update去确保每个 node 上的 pod 版本是最新的
// 还要进行上传快照、清理快照等操作
func newDaemonSetControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.DaemonSetController,
		aliases:  []string{"daemonset"},
		initFunc: startDaemonSetController,
	}
}
func startDaemonSetController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	dsc, err := daemon.NewDaemonSetsController(
		ctx,
		controllerContext.InformerFactory.Apps().V1().DaemonSets(),
		controllerContext.InformerFactory.Apps().V1().ControllerRevisions(),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Core().V1().Nodes(),
		controllerContext.ClientBuilder.ClientOrDie("daemon-set-controller"),
		flowcontrol.NewBackOff(1*time.Second, 15*time.Minute),
	)
	if err != nil {
		return nil, true, fmt.Errorf("error creating DaemonSets controller: %v", err)
	}
	go dsc.Run(ctx, int(controllerContext.ComponentConfig.DaemonSetController.ConcurrentDaemonSetSyncs))
	return nil, true, nil
}

// 1. 创建 Pods
// 确保根据 StatefulSet 的 replicas 字段指定的副本数正确运行。
// 通过确保 Pods 的 固定顺序 和 固定命名，为有状态应用提供稳定性。
// Pod 的名称格式为：<statefulset-name>-<ordinal> （例如，web-0, web-1）。
// 2. 更新 Pods
// 通过 Rolling Update 策略，逐个更新 Pods，保证在有状态应用中保持服务的连续性和正确性。
// 更新的顺序一般为：
// 从最低序号的 Pod 开始；
// 更新完成后，再处理下一个序号的 Pod。
// 3. 扩缩容
// 当 StatefulSet 的 replicas 字段改变时，添加或删除 Pods：
// 添加时按照序号递增的方式（例如，从 web-3 到 web-4）。
// 删除时按照序号递减的方式（例如，从 web-4 到 web-3）。
// 确保扩缩容操作遵循 StatefulSet 的约束，比如 Pod 的依赖顺序。
// 4. 管理存储（PVCs）
// 为每个 Pod 分配独立的 PersistentVolumeClaim (PVC)：
// 每个 Pod 都会获取一个与其关联的独立存储卷；
// 存储卷与 Pod 同序号命名，例如：data-web-0, data-web-1。
// 5. 保证服务稳定
// 确保 StatefulSet 管理的 Pods 与相关的服务（Headless Service）正常绑定。
// Pod 的 DNS 地址会与 Headless Service 结合生成唯一的域名，例如：web-0.<service-name>。
// 这保证了即使 Pod 被重新调度，其名称和存储仍保持稳定。
// 6. 健康检查与恢复
// 监控 StatefulSet 管理的 Pods 的状态，自动恢复失败的 Pod。
// 7. 管理状态一致性
// 确保在 StatefulSet 的所有更新和删除操作中，Pod 的依赖关系得以正确维护。
func newStatefulSetControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.StatefulSetController,
		aliases:  []string{"statefulset"},
		initFunc: startStatefulSetController,
	}
}
func startStatefulSetController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	go statefulset.NewStatefulSetController(
		ctx,
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Apps().V1().StatefulSets(),
		controllerContext.InformerFactory.Core().V1().PersistentVolumeClaims(),
		controllerContext.InformerFactory.Apps().V1().ControllerRevisions(),
		controllerContext.ClientBuilder.ClientOrDie("statefulset-controller"),
	).Run(ctx, int(controllerContext.ComponentConfig.StatefulSetController.ConcurrentStatefulSetSyncs))
	return nil, true, nil
}

// 维持副本数（Replica Count）：
//
// 当期望的副本数（replicas）与实际运行的副本数不一致时，ReplicaSet 会创建或删除 Pod。
// 如果实际数量少于期望值，ReplicaSet 会新建 Pod。
// 如果实际数量多于期望值，ReplicaSet 会删除多余的 Pod。
// 这些 Pod 是通过 Template（spec.template）定义的。
// 基于标签选择器进行管理：
//
// ReplicaSet 通过 selector 找到匹配的 Pod，从而管理它们。
// 如果手动创建了符合选择器的 Pod，ReplicaSet 也会将它们纳入管理。
// 处理节点故障：
//
// 当节点故障导致某些 Pod 无法运行时，ReplicaSet 会重新调度 Pod 确保副本数恢复。
func newReplicaSetControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.ReplicaSetController,
		aliases:  []string{"replicaset"},
		initFunc: startReplicaSetController,
	}
}

func startReplicaSetController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	go replicaset.NewReplicaSetController(
		ctx,
		controllerContext.InformerFactory.Apps().V1().ReplicaSets(),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.ClientBuilder.ClientOrDie("replicaset-controller"),
		replicaset.BurstReplicas,
	).Run(ctx, int(controllerContext.ComponentConfig.ReplicaSetController.ConcurrentRSSyncs))
	return nil, true, nil
}

// 滚动更新（Rolling Update）：
//
// Deployment 支持平滑更新应用而不中断服务。
// 会创建新的 ReplicaSet，同时逐步减少旧的 ReplicaSet 的 Pod 数量，直到完成更新。
// 支持更新策略，如maxUnavailable 和 maxSurge 控制更新速度。
// 可以回滚到先前的版本（旧 ReplicaSet）以处理回退问题。
// 版本历史管理：
//
// Deployment 会保留一定数量的历史版本（旧 ReplicaSet），以便在需要时快速回滚。
// revisionHistoryLimit 配置用于控制历史版本的数量。
// 自动扩缩容：
//
// Deployment 允许动态修改副本数以适应负载变化。
// 通常与 HPA（Horizontal Pod Autoscaler）联动实现自动扩缩容。
// 一致性与稳定性：
//
// Deployment Controller 确保状态符合用户的期望，例如：
// 当有手动干预导致 Pod 状态与期望状态不一致时，会修正为期望的状态。
// 管理更新时中间状态的稳定性，避免波动性故障。
// 多批次分阶段部署：
//
// 部署可以逐步推进，从而降低失败风险。
// 通过配置strategy支持渐进式的更新。
func newDeploymentControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.DeploymentController,
		aliases:  []string{"deployment"},
		initFunc: startDeploymentController,
	}
}

func startDeploymentController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	dc, err := deployment.NewDeploymentController(
		ctx,
		controllerContext.InformerFactory.Apps().V1().Deployments(),
		controllerContext.InformerFactory.Apps().V1().ReplicaSets(),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.ClientBuilder.ClientOrDie("deployment-controller"),
	)
	if err != nil {
		return nil, true, fmt.Errorf("error creating Deployment controller: %v", err)
	}
	go dc.Run(ctx, int(controllerContext.ComponentConfig.DeploymentController.ConcurrentDeploymentSyncs))
	return nil, true, nil
}
