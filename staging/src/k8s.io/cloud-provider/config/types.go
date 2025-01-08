/*
Copyright 2018 The Kubernetes Authors.

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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	nodeconfig "k8s.io/cloud-provider/controllers/node/config"
	serviceconfig "k8s.io/cloud-provider/controllers/service/config"
	cmconfig "k8s.io/controller-manager/config"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CloudControllerManagerConfiguration contains elements describing cloud-controller manager.
type CloudControllerManagerConfiguration struct {
	metav1.TypeMeta

	// Generic holds configuration for a generic controller-manager
	Generic cmconfig.GenericControllerManagerConfiguration
	// KubeCloudSharedConfiguration holds configuration for shared related features
	// both in cloud controller manager and kube-controller manager.
	KubeCloudShared KubeCloudSharedConfiguration

	// NodeController holds configuration for node controller
	// related features.
	NodeController nodeconfig.NodeControllerConfiguration

	// ServiceControllerConfiguration holds configuration for ServiceController
	// related features.
	ServiceController serviceconfig.ServiceControllerConfiguration

	// NodeStatusUpdateFrequency is the frequency at which the controller updates nodes' status
	NodeStatusUpdateFrequency metav1.Duration

	// Webhook is the configuration for cloud-controller-manager hosted webhooks
	Webhook WebhookConfiguration
}

// KubeCloudSharedConfiguration contains elements shared by both kube-controller manager
// and cloud-controller manager, but not genericconfig.
type KubeCloudSharedConfiguration struct {
	// CloudProviderConfiguration holds configuration for CloudProvider related features.
	CloudProvider CloudProviderConfiguration
	// externalCloudVolumePlugin specifies the plugin to use when cloudProvider is "external".
	// It is currently used by the in repo cloud providers to handle node and volume control in the KCM.
	ExternalCloudVolumePlugin string
	// useServiceAccountCredentials indicates whether controllers should be run with
	// individual service account credentials.
	UseServiceAccountCredentials bool
	// run with untagged cloud instances
	AllowUntaggedCloud bool
	// routeReconciliationPeriod is the period for reconciling routes created for Nodes by cloud provider..
	RouteReconciliationPeriod metav1.Duration
	// nodeMonitorPeriod is the period for syncing NodeStatus in CloudNodeLifecycleController.
	NodeMonitorPeriod metav1.Duration
	// clusterName is the instance prefix for the cluster.
	ClusterName string
	// clusterCIDR is CIDR Range for Pods in cluster.
	//ClusterCIDR 是 Kubernetes 中一个重要的配置项，用于定义集群内 Pods 的 IP 地址范围。它的主要功能和意义如下：
	//
	//主要功能
	//Pod IP 地址范围: ClusterCIDR 指定了在 Kubernetes 集群中可以为 Pods 分配的 IP 地址范围。所有在集群中运行的 Pods 都会从这个范围内获取 IP 地址。
	//网络通信: Pods 之间的通信通常依赖于 IP 地址，因此定义 ClusterCIDR 是确保 Pods 能够相互访问的基础。
	//网络插件支持: 不同的网络插件（如 Calico、Flannel、Weave 等）可能会根据 ClusterCIDR 进行配置，以确保正确的网络连接和路由。
	ClusterCIDR string
	// AllocateNodeCIDRs enables CIDRs for Pods to be allocated and, if
	// ConfigureCloudRoutes is true, to be set on the cloud provider.
	// AllocateNodeCIDRs 是 Kubernetes 中的一个配置选项，用于控制是否为每个节点分配 CIDR（Classless Inter-Domain Routing）块，以供 Pod 使用。它的主要功能和作用如下：
	//
	//主要功能
	//节点 CIDR 分配: 如果设置为 true，Kubernetes 将为每个节点分配一个专用的 CIDR 块，这个块中的 IP 地址将用于该节点上运行的 Pod。
	//与云提供商集成: 如果 ConfigureCloudRoutes 也设置为 true，那么 Kubernetes 还会在云提供商上配置相应的路由，以确保流量正确路由到 Pod。
	AllocateNodeCIDRs bool
	// CIDRAllocatorType determines what kind of pod CIDR allocator will be used.
	CIDRAllocatorType string
	// configureCloudRoutes enables CIDRs allocated with allocateNodeCIDRs
	// to be configured on the cloud provider.
	ConfigureCloudRoutes bool
	// nodeSyncPeriod is the period for syncing nodes from cloudprovider. Longer
	// periods will result in fewer calls to cloud provider, but may delay addition
	// of new nodes to cluster.
	NodeSyncPeriod metav1.Duration
}

// CloudProviderConfiguration contains basically elements about cloud provider.
type CloudProviderConfiguration struct {
	// Name is the provider for cloud services.
	Name string
	// cloudConfigFile is the path to the cloud provider configuration file.
	CloudConfigFile string
}

type WebhookConfiguration struct {
	// Webhooks is the list of webhooks to enable or disable
	// '*' means "all enabled by default webhooks"
	// 'foo' means "enable 'foo'"
	// '-foo' means "disable 'foo'"
	// first item for a particular name wins
	Webhooks []string
}
