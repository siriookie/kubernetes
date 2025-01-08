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
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	genericfeatures "k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	restclient "k8s.io/client-go/rest"
	cpnames "k8s.io/cloud-provider/names"
	"k8s.io/component-base/featuregate"
	"k8s.io/controller-manager/controller"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-controller-manager/names"
	pkgcontroller "k8s.io/kubernetes/pkg/controller"
	endpointcontroller "k8s.io/kubernetes/pkg/controller/endpoint"
	"k8s.io/kubernetes/pkg/controller/garbagecollector"
	namespacecontroller "k8s.io/kubernetes/pkg/controller/namespace"
	nodeipamcontroller "k8s.io/kubernetes/pkg/controller/nodeipam"
	nodeipamconfig "k8s.io/kubernetes/pkg/controller/nodeipam/config"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam"
	lifecyclecontroller "k8s.io/kubernetes/pkg/controller/nodelifecycle"
	"k8s.io/kubernetes/pkg/controller/podgc"
	replicationcontroller "k8s.io/kubernetes/pkg/controller/replication"
	"k8s.io/kubernetes/pkg/controller/resourceclaim"
	resourcequotacontroller "k8s.io/kubernetes/pkg/controller/resourcequota"
	serviceaccountcontroller "k8s.io/kubernetes/pkg/controller/serviceaccount"
	"k8s.io/kubernetes/pkg/controller/storageversiongc"
	"k8s.io/kubernetes/pkg/controller/tainteviction"
	ttlcontroller "k8s.io/kubernetes/pkg/controller/ttl"
	"k8s.io/kubernetes/pkg/controller/ttlafterfinished"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach"
	"k8s.io/kubernetes/pkg/controller/volume/ephemeral"
	"k8s.io/kubernetes/pkg/controller/volume/expand"
	persistentvolumecontroller "k8s.io/kubernetes/pkg/controller/volume/persistentvolume"
	"k8s.io/kubernetes/pkg/controller/volume/pvcprotection"
	"k8s.io/kubernetes/pkg/controller/volume/pvprotection"
	"k8s.io/kubernetes/pkg/controller/volume/selinuxwarning"
	"k8s.io/kubernetes/pkg/controller/volume/vacprotection"
	"k8s.io/kubernetes/pkg/features"
	quotainstall "k8s.io/kubernetes/pkg/quota/v1/install"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	"k8s.io/utils/clock"
	netutils "k8s.io/utils/net"
)

const (
	// defaultNodeMaskCIDRIPv4 is default mask size for IPv4 node cidr
	defaultNodeMaskCIDRIPv4 = 24
	// defaultNodeMaskCIDRIPv6 is default mask size for IPv6 node cidr
	defaultNodeMaskCIDRIPv6 = 64
)

func newServiceLBControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:                      cpnames.ServiceLBController,
		aliases:                   []string{"service"},
		initFunc:                  startServiceLBController,
		isCloudProviderController: true,
	}
}

func startServiceLBController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	logger := klog.FromContext(ctx)
	// logger.Info("警告：已设置服务控制器，但在 kube-controller-manager 中没有可用的云提供商功能 (KEP-2395)。将不会配置服务控制器。")
	logger.Info("Warning: service-controller is set, but no cloud provider functionality is available in kube-controller-manger (KEP-2395). Will not configure service controller.")
	return nil, false, nil
}
func newNodeIpamControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.NodeIpamController,
		aliases:  []string{"nodeipam"},
		initFunc: startNodeIpamController,
	}
}

// PAM（IP Address Management，IP 地址管理）是指对网络中的 IP 地址进行规划、分配、管理和监控的过程和工具。它的主要功能包括：
//
// IP 地址分配: 管理和分配 IP 地址给网络设备、服务器和其他资源，确保没有重复分配。
// IP 地址规划: 帮助网络管理员设计和规划 IP 地址空间，以优化网络性能和可扩展性。
// 地址监控: 监控 IP 地址的使用情况，提供关于哪些地址被使用、哪些地址可用的信息。
// 集成 DNS 和 DHCP: IPAM 通常与 DNS（域名系统）和 DHCP（动态主机配置协议）集成，提供更全面的网络管理功能。
// 报告与分析: 提供有关 IP 地址使用情况的报告，帮助管理员做出数据驱动的决策。
func startNodeIpamController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	var serviceCIDR *net.IPNet
	var secondaryServiceCIDR *net.IPNet
	logger := klog.FromContext(ctx)

	// should we start nodeIPAM
	if !controllerContext.ComponentConfig.KubeCloudShared.AllocateNodeCIDRs {
		return nil, false, nil
	}

	if controllerContext.ComponentConfig.KubeCloudShared.CIDRAllocatorType == string(ipam.CloudAllocatorType) {
		// Cannot run cloud ipam controller if cloud provider is nil (--cloud-provider not set or set to 'external')
		return nil, false, errors.New("--cidr-allocator-type is set to 'CloudAllocator' but cloud provider is not configured")
	}

	clusterCIDRs, err := validateCIDRs(controllerContext.ComponentConfig.KubeCloudShared.ClusterCIDR)
	if err != nil {
		return nil, false, err
	}

	// service cidr processing
	if len(strings.TrimSpace(controllerContext.ComponentConfig.NodeIPAMController.ServiceCIDR)) != 0 {
		_, serviceCIDR, err = netutils.ParseCIDRSloppy(controllerContext.ComponentConfig.NodeIPAMController.ServiceCIDR)
		if err != nil {
			logger.Info("Warning: unsuccessful parsing of service CIDR", "CIDR", controllerContext.ComponentConfig.NodeIPAMController.ServiceCIDR, "err", err)
		}
	}

	if len(strings.TrimSpace(controllerContext.ComponentConfig.NodeIPAMController.SecondaryServiceCIDR)) != 0 {
		_, secondaryServiceCIDR, err = netutils.ParseCIDRSloppy(controllerContext.ComponentConfig.NodeIPAMController.SecondaryServiceCIDR)
		if err != nil {
			logger.Info("Warning: unsuccessful parsing of service CIDR", "CIDR", controllerContext.ComponentConfig.NodeIPAMController.SecondaryServiceCIDR, "err", err)
		}
	}

	// the following checks are triggered if both serviceCIDR and secondaryServiceCIDR are provided
	if serviceCIDR != nil && secondaryServiceCIDR != nil {
		// should be dual stack (from different IPFamilies)
		dualstackServiceCIDR, err := netutils.IsDualStackCIDRs([]*net.IPNet{serviceCIDR, secondaryServiceCIDR})
		if err != nil {
			return nil, false, fmt.Errorf("failed to perform dualstack check on serviceCIDR and secondaryServiceCIDR error: %w", err)
		}
		if !dualstackServiceCIDR {
			return nil, false, fmt.Errorf("serviceCIDR and secondaryServiceCIDR are not dualstack (from different IPfamiles)")
		}
	}

	// only --node-cidr-mask-size-ipv4 and --node-cidr-mask-size-ipv6 supported with dual stack clusters.
	// --node-cidr-mask-size flag is incompatible with dual stack clusters.
	nodeCIDRMaskSizes, err := setNodeCIDRMaskSizes(controllerContext.ComponentConfig.NodeIPAMController, clusterCIDRs)
	if err != nil {
		return nil, false, err
	}

	nodeIpamController, err := nodeipamcontroller.NewNodeIpamController(
		ctx,
		controllerContext.InformerFactory.Core().V1().Nodes(),
		nil, // no cloud provider on kube-controller-manager since v1.31 (KEP-2395)
		controllerContext.ClientBuilder.ClientOrDie("node-controller"),
		clusterCIDRs,
		serviceCIDR,
		secondaryServiceCIDR,
		nodeCIDRMaskSizes,
		ipam.CIDRAllocatorType(controllerContext.ComponentConfig.KubeCloudShared.CIDRAllocatorType),
	)
	if err != nil {
		return nil, true, err
	}
	go nodeIpamController.RunWithMetrics(ctx, controllerContext.ControllerManagerMetrics)
	return nil, true, nil
}

// 节点状态监控: NodeLifecycleController 监控节点的状态，包括节点是否可用、是否正常运行等。
// 节点心跳: 通过定期检查节点的健康状况，确保节点在集群中的有效性。如果节点未能在规定时间内发出心跳信号，控制器会将其标记为不可用。
// 节点维护: 处理节点的添加、删除和更新操作，包括标记节点为“不可调度”或“不可用”。
// 自动驱逐: 如果节点长时间不可用，NodeLifecycleController 会自动驱逐在该节点上运行的 Pods，将其迁移到其他健康节点上。
// 节点回收: 处理节点的删除和清理操作，确保在节点被移除后，相关的资源和状态被正确清理。
func newNodeLifecycleControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.NodeLifecycleController,
		aliases:  []string{"nodelifecycle"},
		initFunc: startNodeLifecycleController,
	}
}

func startNodeLifecycleController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	lifecycleController, err := lifecyclecontroller.NewNodeLifecycleController(
		ctx,
		controllerContext.InformerFactory.Coordination().V1().Leases(),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Core().V1().Nodes(),
		controllerContext.InformerFactory.Apps().V1().DaemonSets(),
		// node lifecycle controller uses existing cluster role from node-controller
		controllerContext.ClientBuilder.ClientOrDie("node-controller"),
		controllerContext.ComponentConfig.KubeCloudShared.NodeMonitorPeriod.Duration,
		controllerContext.ComponentConfig.NodeLifecycleController.NodeStartupGracePeriod.Duration,
		controllerContext.ComponentConfig.NodeLifecycleController.NodeMonitorGracePeriod.Duration,
		controllerContext.ComponentConfig.NodeLifecycleController.NodeEvictionRate,
		controllerContext.ComponentConfig.NodeLifecycleController.SecondaryNodeEvictionRate,
		controllerContext.ComponentConfig.NodeLifecycleController.LargeClusterSizeThreshold,
		controllerContext.ComponentConfig.NodeLifecycleController.UnhealthyZoneThreshold,
	)
	if err != nil {
		return nil, true, err
	}
	go lifecycleController.Run(ctx)
	return nil, true, nil
}

func newTaintEvictionControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.TaintEvictionController,
		initFunc: startTaintEvictionController,
		requiredFeatureGates: []featuregate.Feature{
			features.SeparateTaintEvictionController,
		},
	}
}

func startTaintEvictionController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	taintEvictionController, err := tainteviction.New(
		ctx,
		// taint-manager uses existing cluster role from node-controller
		controllerContext.ClientBuilder.ClientOrDie("node-controller"),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Core().V1().Nodes(),
		controllerName,
	)
	if err != nil {
		return nil, false, err
	}
	go taintEvictionController.Run(ctx)
	return nil, true, nil
}

func newCloudNodeLifecycleControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:                      cpnames.CloudNodeLifecycleController,
		aliases:                   []string{"cloud-node-lifecycle"},
		initFunc:                  startCloudNodeLifecycleController,
		isCloudProviderController: true,
	}
}

func startCloudNodeLifecycleController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	logger := klog.FromContext(ctx)
	logger.Info("Warning: node-controller is set, but no cloud provider functionality is available in kube-controller-manger (KEP-2395). Will not configure node lifecyle controller.")
	return nil, false, nil
}

func newNodeRouteControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:                      cpnames.NodeRouteController,
		aliases:                   []string{"route"},
		initFunc:                  startNodeRouteController,
		isCloudProviderController: true,
	}
}

func startNodeRouteController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	logger := klog.FromContext(ctx)
	logger.Info("Warning: configure-cloud-routes is set, but no cloud provider functionality is available in kube-controller-manger (KEP-2395). Will not configure cloud provider routes.")
	return nil, false, nil
}

// PersistentVolumeBinderController 是 Kubernetes 中的一个控制器，主要负责将持久卷（Persistent Volumes，PV）与持久卷声明（Persistent Volume Claims，PVC）进行绑定
func newPersistentVolumeBinderControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.PersistentVolumeBinderController,
		aliases:  []string{"persistentvolume-binder"},
		initFunc: startPersistentVolumeBinderController,
	}
}

func startPersistentVolumeBinderController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	logger := klog.FromContext(ctx)
	plugins, err := ProbeProvisionableRecyclableVolumePlugins(logger, controllerContext.ComponentConfig.PersistentVolumeBinderController.VolumeConfiguration)
	if err != nil {
		return nil, true, fmt.Errorf("failed to probe volume plugins when starting persistentvolume controller: %v", err)
	}

	params := persistentvolumecontroller.ControllerParameters{
		KubeClient:                controllerContext.ClientBuilder.ClientOrDie("persistent-volume-binder"),
		SyncPeriod:                controllerContext.ComponentConfig.PersistentVolumeBinderController.PVClaimBinderSyncPeriod.Duration,
		VolumePlugins:             plugins,
		VolumeInformer:            controllerContext.InformerFactory.Core().V1().PersistentVolumes(),
		ClaimInformer:             controllerContext.InformerFactory.Core().V1().PersistentVolumeClaims(),
		ClassInformer:             controllerContext.InformerFactory.Storage().V1().StorageClasses(),
		PodInformer:               controllerContext.InformerFactory.Core().V1().Pods(),
		NodeInformer:              controllerContext.InformerFactory.Core().V1().Nodes(),
		EnableDynamicProvisioning: controllerContext.ComponentConfig.PersistentVolumeBinderController.VolumeConfiguration.EnableDynamicProvisioning,
	}
	volumeController, volumeControllerErr := persistentvolumecontroller.NewController(ctx, params)
	if volumeControllerErr != nil {
		return nil, true, fmt.Errorf("failed to construct persistentvolume controller: %v", volumeControllerErr)
	}
	go volumeController.Run(ctx)
	return nil, true, nil
}

// Pod 创建时：
// 1. Pod 被调度到特定节点
// 2. AD Controller 检测到 Pod 使用的 PV
// 3. 触发该 PV 到目标节点的 Attach 操作
// 4. 等待 Attach 完成
// 5. 更新 VolumeAttachment 对象状态
//
// Pod 删除时：
// 1. 检测 Pod 删除事件
// 2. 确认没有其他 Pod 使用该 PV
// 3. 触发 Detach 操作
// 4. 等待 Detach 完成
// 5. 清理 VolumeAttachment 对象
func newPersistentVolumeAttachDetachControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.PersistentVolumeAttachDetachController,
		aliases:  []string{"attachdetach"},
		initFunc: startPersistentVolumeAttachDetachController,
	}
}

func startPersistentVolumeAttachDetachController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	logger := klog.FromContext(ctx)
	csiNodeInformer := controllerContext.InformerFactory.Storage().V1().CSINodes()
	csiDriverInformer := controllerContext.InformerFactory.Storage().V1().CSIDrivers()

	plugins, err := ProbeAttachableVolumePlugins(logger, controllerContext.ComponentConfig.PersistentVolumeBinderController.VolumeConfiguration)
	if err != nil {
		return nil, true, fmt.Errorf("failed to probe volume plugins when starting attach/detach controller: %v", err)
	}

	ctx = klog.NewContext(ctx, logger)
	attachDetachController, attachDetachControllerErr :=
		attachdetach.NewAttachDetachController(
			ctx,
			controllerContext.ClientBuilder.ClientOrDie("attachdetach-controller"),
			controllerContext.InformerFactory.Core().V1().Pods(),
			controllerContext.InformerFactory.Core().V1().Nodes(),
			controllerContext.InformerFactory.Core().V1().PersistentVolumeClaims(),
			controllerContext.InformerFactory.Core().V1().PersistentVolumes(),
			csiNodeInformer,
			csiDriverInformer,
			controllerContext.InformerFactory.Storage().V1().VolumeAttachments(),
			plugins,
			GetDynamicPluginProber(controllerContext.ComponentConfig.PersistentVolumeBinderController.VolumeConfiguration),
			controllerContext.ComponentConfig.AttachDetachController.DisableAttachDetachReconcilerSync,
			controllerContext.ComponentConfig.AttachDetachController.ReconcilerSyncLoopPeriod.Duration,
			controllerContext.ComponentConfig.AttachDetachController.DisableForceDetachOnTimeout,
			attachdetach.DefaultTimerConfig,
		)
	if attachDetachControllerErr != nil {
		return nil, true, fmt.Errorf("failed to start attach/detach controller: %v", attachDetachControllerErr)
	}
	go attachDetachController.Run(ctx)
	return nil, true, nil
}

func newPersistentVolumeExpanderControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.PersistentVolumeExpanderController,
		aliases:  []string{"persistentvolume-expander"},
		initFunc: startPersistentVolumeExpanderController,
	}
}

func startPersistentVolumeExpanderController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	logger := klog.FromContext(ctx)
	plugins, err := ProbeExpandableVolumePlugins(logger, controllerContext.ComponentConfig.PersistentVolumeBinderController.VolumeConfiguration)
	if err != nil {
		return nil, true, fmt.Errorf("failed to probe volume plugins when starting volume expand controller: %v", err)
	}
	csiTranslator := csitrans.New()

	expandController, expandControllerErr := expand.NewExpandController(
		ctx,
		controllerContext.ClientBuilder.ClientOrDie("expand-controller"),
		controllerContext.InformerFactory.Core().V1().PersistentVolumeClaims(),
		plugins,
		csiTranslator,
		csimigration.NewPluginManager(csiTranslator, utilfeature.DefaultFeatureGate),
	)

	if expandControllerErr != nil {
		return nil, true, fmt.Errorf("failed to start volume expand controller: %v", expandControllerErr)
	}
	go expandController.Run(ctx)
	return nil, true, nil
}

// 监听pod和pvc，如果类型是Ephemeral volumes （临时卷），则按需创建pvc
func newEphemeralVolumeControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.EphemeralVolumeController,
		aliases:  []string{"ephemeral-volume"},
		initFunc: startEphemeralVolumeController,
	}
}

func startEphemeralVolumeController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	ephemeralController, err := ephemeral.NewController(
		ctx,
		controllerContext.ClientBuilder.ClientOrDie("ephemeral-volume-controller"),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Core().V1().PersistentVolumeClaims())
	if err != nil {
		return nil, true, fmt.Errorf("failed to start ephemeral volume controller: %v", err)
	}
	go ephemeralController.Run(ctx, int(controllerContext.ComponentConfig.EphemeralVolumeController.ConcurrentEphemeralVolumeSyncs))
	return nil, true, nil
}

const defaultResourceClaimControllerWorkers = 10

// 资源声明（ResourceClaim）是一个资源管理对象，允许用户和系统声明对资源（如存储、计算等）的需求
func newResourceClaimControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.ResourceClaimController,
		aliases:  []string{"resource-claim-controller"},
		initFunc: startResourceClaimController,
		requiredFeatureGates: []featuregate.Feature{
			features.DynamicResourceAllocation, // TODO update app.TestFeatureGatedControllersShouldNotDefineAliases when removing this feature
		},
	}
}

func startResourceClaimController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	ephemeralController, err := resourceclaim.NewController(
		klog.FromContext(ctx),
		utilfeature.DefaultFeatureGate.Enabled(features.DRAAdminAccess),
		controllerContext.ClientBuilder.ClientOrDie("resource-claim-controller"),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Resource().V1beta1().ResourceClaims(),
		controllerContext.InformerFactory.Resource().V1beta1().ResourceClaimTemplates())
	if err != nil {
		return nil, true, fmt.Errorf("failed to start resource claim controller: %v", err)
	}
	go ephemeralController.Run(ctx, defaultResourceClaimControllerWorkers)
	return nil, true, nil
}

func newEndpointsControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.EndpointsController,
		aliases:  []string{"endpoint"},
		initFunc: startEndpointsController,
	}
}

func startEndpointsController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	go endpointcontroller.NewEndpointController(
		ctx,
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Core().V1().Services(),
		controllerContext.InformerFactory.Core().V1().Endpoints(),
		controllerContext.ClientBuilder.ClientOrDie("endpoint-controller"),
		controllerContext.ComponentConfig.EndpointController.EndpointUpdatesBatchPeriod.Duration,
	).Run(ctx, int(controllerContext.ComponentConfig.EndpointController.ConcurrentEndpointSyncs))
	return nil, true, nil
}

func newReplicationControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.ReplicationControllerController,
		aliases:  []string{"replicationcontroller"},
		initFunc: startReplicationController,
	}
}

func startReplicationController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	go replicationcontroller.NewReplicationManager(
		ctx,
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Core().V1().ReplicationControllers(),
		controllerContext.ClientBuilder.ClientOrDie("replication-controller"),
		replicationcontroller.BurstReplicas,
	).Run(ctx, int(controllerContext.ComponentConfig.ReplicationController.ConcurrentRCSyncs))
	return nil, true, nil
}

func newPodGarbageCollectorControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.PodGarbageCollectorController,
		aliases:  []string{"podgc"},
		initFunc: startPodGarbageCollectorController,
	}
}

func startPodGarbageCollectorController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	go podgc.NewPodGC(
		ctx,
		controllerContext.ClientBuilder.ClientOrDie("pod-garbage-collector"),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.InformerFactory.Core().V1().Nodes(),
		int(controllerContext.ComponentConfig.PodGCController.TerminatedPodGCThreshold),
	).Run(ctx)
	return nil, true, nil
}

func newResourceQuotaControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.ResourceQuotaController,
		aliases:  []string{"resourcequota"},
		initFunc: startResourceQuotaController,
	}
}

func startResourceQuotaController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	resourceQuotaControllerClient := controllerContext.ClientBuilder.ClientOrDie("resourcequota-controller")
	resourceQuotaControllerDiscoveryClient := controllerContext.ClientBuilder.DiscoveryClientOrDie("resourcequota-controller")
	discoveryFunc := resourceQuotaControllerDiscoveryClient.ServerPreferredNamespacedResources
	listerFuncForResource := generic.ListerFuncForResourceFunc(controllerContext.InformerFactory.ForResource)
	quotaConfiguration := quotainstall.NewQuotaConfigurationForControllers(listerFuncForResource)

	resourceQuotaControllerOptions := &resourcequotacontroller.ControllerOptions{
		QuotaClient:               resourceQuotaControllerClient.CoreV1(),
		ResourceQuotaInformer:     controllerContext.InformerFactory.Core().V1().ResourceQuotas(),
		ResyncPeriod:              pkgcontroller.StaticResyncPeriodFunc(controllerContext.ComponentConfig.ResourceQuotaController.ResourceQuotaSyncPeriod.Duration),
		InformerFactory:           controllerContext.ObjectOrMetadataInformerFactory,
		ReplenishmentResyncPeriod: controllerContext.ResyncPeriod,
		DiscoveryFunc:             discoveryFunc,
		IgnoredResourcesFunc:      quotaConfiguration.IgnoredResources,
		InformersStarted:          controllerContext.InformersStarted,
		Registry:                  generic.NewRegistry(quotaConfiguration.Evaluators()),
		UpdateFilter:              quotainstall.DefaultUpdateFilter(),
	}
	resourceQuotaController, err := resourcequotacontroller.NewController(ctx, resourceQuotaControllerOptions)
	if err != nil {
		return nil, false, err
	}
	go resourceQuotaController.Run(ctx, int(controllerContext.ComponentConfig.ResourceQuotaController.ConcurrentResourceQuotaSyncs))

	// Periodically the quota controller to detect new resource types
	go resourceQuotaController.Sync(ctx, discoveryFunc, 30*time.Second)

	return nil, true, nil
}

func newNamespaceControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.NamespaceController,
		aliases:  []string{"namespace"},
		initFunc: startNamespaceController,
	}
}

func startNamespaceController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	// the namespace cleanup controller is very chatty.  It makes lots of discovery calls and then it makes lots of delete calls
	// the ratelimiter negatively affects its speed.  Deleting 100 total items in a namespace (that's only a few of each resource
	// including events), takes ~10 seconds by default.
	//命名空间清理控制器（Namespace Cleanup Controller）会频繁地执行资源发现和删除操作。速率限制器（Rate Limiter）的存在会降低其执行速度。
	//例如，删除一个命名空间中总计 100 个项目（每种资源仅包含少量对象，包括事件）通常需要约 10 秒的时间。
	nsKubeconfig := controllerContext.ClientBuilder.ConfigOrDie("namespace-controller")
	nsKubeconfig.QPS *= 20
	nsKubeconfig.Burst *= 100
	namespaceKubeClient := clientset.NewForConfigOrDie(nsKubeconfig)
	return startModifiedNamespaceController(ctx, controllerContext, namespaceKubeClient, nsKubeconfig)
}

func startModifiedNamespaceController(ctx context.Context, controllerContext ControllerContext, namespaceKubeClient clientset.Interface, nsKubeconfig *restclient.Config) (controller.Interface, bool, error) {

	metadataClient, err := metadata.NewForConfig(nsKubeconfig)
	if err != nil {
		return nil, true, err
	}

	discoverResourcesFn := namespaceKubeClient.Discovery().ServerPreferredNamespacedResources

	namespaceController := namespacecontroller.NewNamespaceController(
		ctx,
		namespaceKubeClient,
		metadataClient,
		discoverResourcesFn,
		controllerContext.InformerFactory.Core().V1().Namespaces(),
		controllerContext.ComponentConfig.NamespaceController.NamespaceSyncPeriod.Duration,
		v1.FinalizerKubernetes,
	)
	go namespaceController.Run(ctx, int(controllerContext.ComponentConfig.NamespaceController.ConcurrentNamespaceSyncs))

	return nil, true, nil
}

func newServiceAccountControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.ServiceAccountController,
		aliases:  []string{"serviceaccount"},
		initFunc: startServiceAccountController,
	}
}

func startServiceAccountController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	sac, err := serviceaccountcontroller.NewServiceAccountsController(
		controllerContext.InformerFactory.Core().V1().ServiceAccounts(),
		controllerContext.InformerFactory.Core().V1().Namespaces(),
		controllerContext.ClientBuilder.ClientOrDie("service-account-controller"),
		serviceaccountcontroller.DefaultServiceAccountsControllerOptions(),
	)
	if err != nil {
		return nil, true, fmt.Errorf("error creating ServiceAccount controller: %v", err)
	}
	go sac.Run(ctx, 1)
	return nil, true, nil
}

func newTTLControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.TTLController,
		aliases:  []string{"ttl"},
		initFunc: startTTLController,
	}
}

func startTTLController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	go ttlcontroller.NewTTLController(
		ctx,
		controllerContext.InformerFactory.Core().V1().Nodes(),
		controllerContext.ClientBuilder.ClientOrDie("ttl-controller"),
	).Run(ctx, 5)
	return nil, true, nil
}

func newGarbageCollectorControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.GarbageCollectorController,
		aliases:  []string{"garbagecollector"},
		initFunc: startGarbageCollectorController,
	}
}

func startGarbageCollectorController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	if !controllerContext.ComponentConfig.GarbageCollectorController.EnableGarbageCollector {
		return nil, false, nil
	}

	gcClientset := controllerContext.ClientBuilder.ClientOrDie("generic-garbage-collector")
	discoveryClient := controllerContext.ClientBuilder.DiscoveryClientOrDie("generic-garbage-collector")

	config := controllerContext.ClientBuilder.ConfigOrDie("generic-garbage-collector")
	// Increase garbage collector controller's throughput: each object deletion takes two API calls,
	// so to get |config.QPS| deletion rate we need to allow 2x more requests for this controller.
	config.QPS *= 2
	metadataClient, err := metadata.NewForConfig(config)
	if err != nil {
		return nil, true, err
	}

	garbageCollector, err := garbagecollector.NewComposedGarbageCollector(
		ctx,
		gcClientset,
		metadataClient,
		controllerContext.RESTMapper,
		controllerContext.GraphBuilder,
	)
	if err != nil {
		return nil, true, fmt.Errorf("failed to start the generic garbage collector: %w", err)
	}

	// Start the garbage collector.
	workers := int(controllerContext.ComponentConfig.GarbageCollectorController.ConcurrentGCSyncs)
	const syncPeriod = 30 * time.Second
	go garbageCollector.Run(ctx, workers, syncPeriod)

	// Periodically refresh the RESTMapper with new discovery information and sync
	// the garbage collector.
	go garbageCollector.Sync(ctx, discoveryClient, syncPeriod)

	return garbageCollector, true, nil
}

// PersistentVolumeClaimProtection 控制器通过添加 finalizer 来防止 PVC 在关联资源（如 PV）的清理工作未完成时被删除。
// 通过这种机制，可以确保 PVC 在被删除之前，相关的资源和依赖关系得以正确处理，从而避免潜在的数据丢失或其他不一致的情况。
// 核心就是通过添加 "finalizer" 来防止 PVC 被意外删除，直到所有相关的资源和清理工作完成。
func newPersistentVolumeClaimProtectionControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.PersistentVolumeClaimProtectionController,
		aliases:  []string{"pvc-protection"},
		initFunc: startPersistentVolumeClaimProtectionController,
	}
}

func startPersistentVolumeClaimProtectionController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	pvcProtectionController, err := pvcprotection.NewPVCProtectionController(
		klog.FromContext(ctx),
		controllerContext.InformerFactory.Core().V1().PersistentVolumeClaims(),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.ClientBuilder.ClientOrDie("pvc-protection-controller"),
	)
	if err != nil {
		return nil, true, fmt.Errorf("failed to start the pvc protection controller: %v", err)
	}
	go pvcProtectionController.Run(ctx, 1)
	return nil, true, nil
}

// 给pv加finalize来进行删除保护
func newPersistentVolumeProtectionControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.PersistentVolumeProtectionController,
		aliases:  []string{"pv-protection"},
		initFunc: startPersistentVolumeProtectionController,
	}
}

func startPersistentVolumeProtectionController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	go pvprotection.NewPVProtectionController(
		klog.FromContext(ctx),
		controllerContext.InformerFactory.Core().V1().PersistentVolumes(),
		controllerContext.ClientBuilder.ClientOrDie("pv-protection-controller"),
	).Run(ctx, 1)
	return nil, true, nil
}

// 给vac加finalize来进行删除保护
func newVolumeAttributesClassProtectionControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.VolumeAttributesClassProtectionController,
		initFunc: startVolumeAttributesClassProtectionController,
		requiredFeatureGates: []featuregate.Feature{
			features.VolumeAttributesClass,
		},
	}
}

func startVolumeAttributesClassProtectionController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	vacProtectionController, err := vacprotection.NewVACProtectionController(
		klog.FromContext(ctx),
		controllerContext.ClientBuilder.ClientOrDie("volumeattributesclass-protection-controller"),
		controllerContext.InformerFactory.Core().V1().PersistentVolumeClaims(),
		controllerContext.InformerFactory.Core().V1().PersistentVolumes(),
		controllerContext.InformerFactory.Storage().V1beta1().VolumeAttributesClasses(),
	)
	if err != nil {
		return nil, true, fmt.Errorf("failed to start the vac protection controller: %w", err)
	}
	go vacProtectionController.Run(ctx, 1)
	return nil, true, nil
}

// 监听job的变动，删除已完成的并且ttl已经到了的job
func newTTLAfterFinishedControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.TTLAfterFinishedController,
		aliases:  []string{"ttl-after-finished"},
		initFunc: startTTLAfterFinishedController,
	}
}

func startTTLAfterFinishedController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	go ttlafterfinished.New(
		ctx,
		controllerContext.InformerFactory.Batch().V1().Jobs(),
		controllerContext.ClientBuilder.ClientOrDie("ttl-after-finished-controller"),
	).Run(ctx, int(controllerContext.ComponentConfig.TTLAfterFinishedController.ConcurrentTTLSyncs))
	return nil, true, nil
}

func newLegacyServiceAccountTokenCleanerControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.LegacyServiceAccountTokenCleanerController,
		aliases:  []string{"legacy-service-account-token-cleaner"},
		initFunc: startLegacyServiceAccountTokenCleanerController,
	}
}

func startLegacyServiceAccountTokenCleanerController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	cleanUpPeriod := controllerContext.ComponentConfig.LegacySATokenCleaner.CleanUpPeriod.Duration
	legacySATokenCleaner, err := serviceaccountcontroller.NewLegacySATokenCleaner(
		controllerContext.InformerFactory.Core().V1().ServiceAccounts(),
		controllerContext.InformerFactory.Core().V1().Secrets(),
		controllerContext.InformerFactory.Core().V1().Pods(),
		controllerContext.ClientBuilder.ClientOrDie("legacy-service-account-token-cleaner"),
		clock.RealClock{},
		serviceaccountcontroller.LegacySATokenCleanerOptions{
			CleanUpPeriod: cleanUpPeriod,
			SyncInterval:  serviceaccountcontroller.DefaultCleanerSyncInterval,
		})
	if err != nil {
		return nil, true, fmt.Errorf("failed to start the legacy service account token cleaner: %v", err)
	}
	go legacySATokenCleaner.Run(ctx)
	return nil, true, nil
}

// processCIDRs is a helper function that works on a comma separated cidrs and returns
// a list of typed cidrs
// error if failed to parse any of the cidrs or invalid length of cidrs
func validateCIDRs(cidrsList string) ([]*net.IPNet, error) {
	// failure: bad cidrs in config
	clusterCIDRs, dualStack, err := processCIDRs(cidrsList)
	if err != nil {
		return nil, err
	}

	// failure: more than one cidr but they are not configured as dual stack
	if len(clusterCIDRs) > 1 && !dualStack {
		return nil, fmt.Errorf("len of ClusterCIDRs==%v and they are not configured as dual stack (at least one from each IPFamily", len(clusterCIDRs))
	}

	// failure: more than cidrs is not allowed even with dual stack
	if len(clusterCIDRs) > 2 {
		return nil, fmt.Errorf("length of clusterCIDRs is:%v more than max allowed of 2", len(clusterCIDRs))
	}

	return clusterCIDRs, nil
}

// processCIDRs is a helper function that works on a comma separated cidrs and returns
// a list of typed cidrs
// a flag if cidrs represents a dual stack
// error if failed to parse any of the cidrs
func processCIDRs(cidrsList string) ([]*net.IPNet, bool, error) {
	cidrsSplit := strings.Split(strings.TrimSpace(cidrsList), ",")

	cidrs, err := netutils.ParseCIDRs(cidrsSplit)
	if err != nil {
		return nil, false, err
	}

	// if cidrs has an error then the previous call will fail
	// safe to ignore error checking on next call
	dualstack, _ := netutils.IsDualStackCIDRs(cidrs)

	return cidrs, dualstack, nil
}

// setNodeCIDRMaskSizes returns the IPv4 and IPv6 node cidr mask sizes to the value provided
// for --node-cidr-mask-size-ipv4 and --node-cidr-mask-size-ipv6 respectively. If value not provided,
// then it will return default IPv4 and IPv6 cidr mask sizes.
func setNodeCIDRMaskSizes(cfg nodeipamconfig.NodeIPAMControllerConfiguration, clusterCIDRs []*net.IPNet) ([]int, error) {

	sortedSizes := func(maskSizeIPv4, maskSizeIPv6 int) []int {
		nodeMaskCIDRs := make([]int, len(clusterCIDRs))

		for idx, clusterCIDR := range clusterCIDRs {
			if netutils.IsIPv6CIDR(clusterCIDR) {
				nodeMaskCIDRs[idx] = maskSizeIPv6
			} else {
				nodeMaskCIDRs[idx] = maskSizeIPv4
			}
		}
		return nodeMaskCIDRs
	}

	// --node-cidr-mask-size flag is incompatible with dual stack clusters.
	ipv4Mask, ipv6Mask := defaultNodeMaskCIDRIPv4, defaultNodeMaskCIDRIPv6
	isDualstack := len(clusterCIDRs) > 1

	// case one: cluster is dualstack (i.e, more than one cidr)
	if isDualstack {
		// if --node-cidr-mask-size then fail, user must configure the correct dual-stack mask sizes (or use default)
		if cfg.NodeCIDRMaskSize != 0 {
			return nil, errors.New("usage of --node-cidr-mask-size is not allowed with dual-stack clusters")

		}

		if cfg.NodeCIDRMaskSizeIPv4 != 0 {
			ipv4Mask = int(cfg.NodeCIDRMaskSizeIPv4)
		}
		if cfg.NodeCIDRMaskSizeIPv6 != 0 {
			ipv6Mask = int(cfg.NodeCIDRMaskSizeIPv6)
		}
		return sortedSizes(ipv4Mask, ipv6Mask), nil
	}

	maskConfigured := cfg.NodeCIDRMaskSize != 0
	maskV4Configured := cfg.NodeCIDRMaskSizeIPv4 != 0
	maskV6Configured := cfg.NodeCIDRMaskSizeIPv6 != 0
	isSingleStackIPv6 := netutils.IsIPv6CIDR(clusterCIDRs[0])

	// original flag is set
	if maskConfigured {
		// original mask flag is still the main reference.
		if maskV4Configured || maskV6Configured {
			return nil, errors.New("usage of --node-cidr-mask-size-ipv4 and --node-cidr-mask-size-ipv6 is not allowed if --node-cidr-mask-size is set. For dual-stack clusters please unset it and use IPFamily specific flags")
		}

		mask := int(cfg.NodeCIDRMaskSize)
		return sortedSizes(mask, mask), nil
	}

	if maskV4Configured {
		if isSingleStackIPv6 {
			return nil, errors.New("usage of --node-cidr-mask-size-ipv4 is not allowed for a single-stack IPv6 cluster")
		}

		ipv4Mask = int(cfg.NodeCIDRMaskSizeIPv4)
	}

	// !maskV4Configured && !maskConfigured && maskV6Configured
	if maskV6Configured {
		if !isSingleStackIPv6 {
			return nil, errors.New("usage of --node-cidr-mask-size-ipv6 is not allowed for a single-stack IPv4 cluster")
		}

		ipv6Mask = int(cfg.NodeCIDRMaskSizeIPv6)
	}
	return sortedSizes(ipv4Mask, ipv6Mask), nil
}

func newStorageVersionGarbageCollectorControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:     names.StorageVersionGarbageCollectorController,
		aliases:  []string{"storage-version-gc"},
		initFunc: startStorageVersionGarbageCollectorController,
		requiredFeatureGates: []featuregate.Feature{
			genericfeatures.APIServerIdentity,
			genericfeatures.StorageVersionAPI,
		},
	}
}

// 在 Kubernetes 中，租赁概念以Lease为代表 coordination.k8s.io API组中的对象，
// 用于系统关键功能，例如节点心跳和组件级领导者选举。
// 在 Kubernetes 中，StorageVersion 是用于跟踪不同 API 服务器对存储资源的版本和编码方式的对象。每个 API 服务器可能有不同的存储版本信息，这些信息通常在 API 服务器内部存储，并记录了该服务器使用的存储版本以及其编码方式。
// ------------------------------------------------------
// ServerStorageVersion 是一个结构体，通常包含以下内容：
//
// APIServerID：标识 API 服务器的唯一标识符，用来跟踪该服务器的存储版本信息。
// StorageVersion：表示该 API 服务器的存储版本信息，可能包含存储的格式、编码方式等。
// 在这段代码中，serverStorageVersions 代表的是一个列表，包含多个 ServerStorageVersion 记录，每一条记录都代表某个 API 服务器的存储版本信息。该列表通常会与存储版本对象（StorageVersion）相关联，用来跟踪集群中不同 API 服务器的存储版本状态。
func startStorageVersionGarbageCollectorController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	go storageversiongc.NewStorageVersionGC(
		ctx,
		controllerContext.ClientBuilder.ClientOrDie("storage-version-garbage-collector"),
		controllerContext.InformerFactory.Coordination().V1().Leases(),
		controllerContext.InformerFactory.Internal().V1alpha1().StorageVersions(),
	).Run(ctx)
	return nil, true, nil
}

func newSELinuxWarningControllerDescriptor() *ControllerDescriptor {
	return &ControllerDescriptor{
		name:                names.SELinuxWarningController,
		initFunc:            startSELinuxWarningController,
		isDisabledByDefault: true,
		requiredFeatureGates: []featuregate.Feature{
			features.SELinuxChangePolicy,
		},
	}
}

func startSELinuxWarningController(ctx context.Context, controllerContext ControllerContext, controllerName string) (controller.Interface, bool, error) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.SELinuxChangePolicy) {
		return nil, false, nil
	}

	logger := klog.FromContext(ctx)
	csiDriverInformer := controllerContext.InformerFactory.Storage().V1().CSIDrivers()
	plugins, err := ProbePersistentVolumePlugins(logger, controllerContext.ComponentConfig.PersistentVolumeBinderController.VolumeConfiguration)
	if err != nil {
		return nil, true, fmt.Errorf("failed to probe volume plugins when starting SELinux warning controller: %w", err)
	}

	seLinuxController, err :=
		selinuxwarning.NewController(
			ctx,
			controllerContext.ClientBuilder.ClientOrDie(controllerName),
			controllerContext.InformerFactory.Core().V1().Pods(),
			controllerContext.InformerFactory.Core().V1().PersistentVolumeClaims(),
			controllerContext.InformerFactory.Core().V1().PersistentVolumes(),
			csiDriverInformer,
			plugins,
			GetDynamicPluginProber(controllerContext.ComponentConfig.PersistentVolumeBinderController.VolumeConfiguration),
		)
	if err != nil {
		return nil, true, fmt.Errorf("failed to start SELinux warning controller: %w", err)
	}
	go seLinuxController.Run(ctx, 1)
	return nil, true, nil
}
