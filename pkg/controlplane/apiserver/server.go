/*
Copyright 2023 The Kubernetes Authors.

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

package apiserver

import (
	"context"
	"fmt"
	"os"
	"time"

	coordinationapiv1 "k8s.io/api/coordination/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	apiserverfeatures "k8s.io/apiserver/pkg/features"
	peerreconcilers "k8s.io/apiserver/pkg/reconcilers"
	genericregistry "k8s.io/apiserver/pkg/registry/generic"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	"k8s.io/component-base/zpages/flagz"
	"k8s.io/component-base/zpages/statusz"
	"k8s.io/component-helpers/apimachinery/lease"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	"k8s.io/kubernetes/pkg/controlplane/controller/apiserverleasegc"
	"k8s.io/kubernetes/pkg/controlplane/controller/clusterauthenticationtrust"
	"k8s.io/kubernetes/pkg/controlplane/controller/leaderelection"
	"k8s.io/kubernetes/pkg/controlplane/controller/legacytokentracking"
	"k8s.io/kubernetes/pkg/controlplane/controller/systemnamespaces"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/routes"
	"k8s.io/kubernetes/pkg/serviceaccount"
)

var (
	// IdentityLeaseGCPeriod is the interval which the lease GC controller checks for expired leases
	// IdentityLeaseGCPeriod is exposed so integration tests can tune this value.
	IdentityLeaseGCPeriod = 3600 * time.Second
	// IdentityLeaseDurationSeconds is the duration of kube-apiserver lease in seconds
	// IdentityLeaseDurationSeconds is exposed so integration tests can tune this value.
	IdentityLeaseDurationSeconds = 3600
	// IdentityLeaseRenewIntervalPeriod is the interval of kube-apiserver renewing its lease in seconds
	// IdentityLeaseRenewIntervalPeriod is exposed so integration tests can tune this value.
	IdentityLeaseRenewIntervalPeriod = 10 * time.Second

	// LeaseCandidateGCPeriod is the interval which the leasecandidate GC controller checks for expired leases
	// This is exposed so integration tests can tune this value.
	LeaseCandidateGCPeriod = 30 * time.Minute
)

const (
	// IdentityLeaseComponentLabelKey is used to apply a component label to identity lease objects, indicating:
	//   1. the lease is an identity lease (different from leader election leases)
	//   2. which component owns this lease
	IdentityLeaseComponentLabelKey = "apiserver.kubernetes.io/identity"
)

// Server is a struct that contains a generic control plane apiserver instance
// that can be run to start serving the APIs.
type Server struct {
	GenericAPIServer *genericapiserver.GenericAPIServer

	APIResourceConfigSource   serverstorage.APIResourceConfigSource
	RESTOptionsGetter         genericregistry.RESTOptionsGetter
	ClusterAuthenticationInfo clusterauthenticationtrust.ClusterAuthenticationInfo
	VersionedInformers        clientgoinformers.SharedInformerFactory
}

// New returns a new instance of Master from the given config.
// Certain config fields will be set to a default value if unset.
// Certain config fields must be specified, including:
// KubeletClientConfig
// 基于配置 completedConfig 创建并初始化一个完整的 kube-apiserver（或称 Server）实例，
// 并将其各种控制器、钩子、OpenAPI 接口、认证机制等按需启动和注册到 GenericAPIServer 上。
func (c completedConfig) New(name string, delegationTarget genericapiserver.DelegationTarget) (*Server, error) {
	//创建了一个 GenericAPIServer，这是构建 Kubernetes API Server 的基础结构，封装了 HTTP 服务、认证、授权、审计、中间件注册等核心组件。
	generic, err := c.Generic.New(name, delegationTarget)
	if err != nil {
		return nil, err
	}
	//如果启用了日志支持功能，则会注册 /logs 路由用于访问容器日志。
	if c.EnableLogsSupport {
		routes.Logs{}.Install(generic.Handler.GoRestfulContainer)
	}
	//如果配置了 ServiceAccountIssuerURL，
	//则暴露 OIDC Discovery 端点（如：/.well-known/openid-configuration），用于支持 OIDC 格式的 ServiceAccount Token 验证。
	md, err := serviceaccount.NewOpenIDMetadataProvider(
		c.ServiceAccountIssuerURL,
		c.ServiceAccountJWKSURI,
		c.Generic.ExternalAddress,
		c.ServiceAccountPublicKeysGetter,
	)
	if err != nil {
		// If there was an error, skip installing the endpoints and log the
		// error, but continue on. We don't return the error because the
		// metadata responses require additional, backwards incompatible
		// validation of command-line options.
		msg := fmt.Sprintf("Could not construct pre-rendered responses for"+
			" ServiceAccountIssuerDiscovery endpoints. Endpoints will not be"+
			" enabled. Error: %v", err)
		if c.ServiceAccountIssuerURL != "" {
			// The user likely expects this feature to be enabled if issuer URL is
			// set and the feature gate is enabled. In the future, if there is no
			// longer a feature gate and issuer URL is not set, the user may not
			// expect this feature to be enabled. We log the former case as an Error
			// and the latter case as an Info.
			klog.Error(msg)
		} else {
			klog.Info(msg)
		}
	} else {
		routes.NewOpenIDMetadataServer(md).Install(generic.Handler.GoRestfulContainer)
	}

	s := &Server{
		GenericAPIServer: generic,

		APIResourceConfigSource:   c.APIResourceConfigSource,
		RESTOptionsGetter:         c.Generic.RESTOptionsGetter,
		ClusterAuthenticationInfo: c.ClusterAuthenticationInfo,
		VersionedInformers:        c.VersionedInformers,
	}
	//用于控制器与自身 apiserver 通信的 loopback 客户端。
	client, err := kubernetes.NewForConfig(s.GenericAPIServer.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}
	//用于确保一些关键系统命名空间（如 kube-system）在 apiserver 启动后被创建出来。
	//在 Kubernetes 中，除了 kube-controller-manager 中的各种核心 controller（如 DeploymentController、NodeController 等），
	//kube-apiserver 本身也运行了不少 controller，它们主要用于维护和增强 API Server 的可用性、安全性、以及和其他组件的协作。
	if len(c.SystemNamespaces) > 0 {
		s.GenericAPIServer.AddPostStartHookOrDie("start-system-namespaces-controller", func(hookContext genericapiserver.PostStartHookContext) error {
			go systemnamespaces.NewController(c.SystemNamespaces, client, s.VersionedInformers.Core().V1().Namespaces()).Run(hookContext.Done())
			return nil
		})
	}

	_, publicServicePort, err := c.Generic.SecureServing.HostPort()
	if err != nil {
		return nil, fmt.Errorf("failed to get listener address: %w", err)
	}
	//ZPages 是 Google 提出的调试页面：flagz 显示命令行 flag 状态，statusz 显示状态指标。
	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		if c.Generic.Flagz != nil {
			flagz.Install(s.GenericAPIServer.Handler.NonGoRestfulMux, name, c.Generic.Flagz)
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentStatusz) {
		statusz.Install(s.GenericAPIServer.Handler.NonGoRestfulMux, name, statusz.NewRegistry())
	}

	if utilfeature.DefaultFeatureGate.Enabled(apiserverfeatures.CoordinatedLeaderElection) {
		leaseInformer := s.VersionedInformers.Coordination().V1().Leases()
		lcInformer := s.VersionedInformers.Coordination().V1alpha2().LeaseCandidates()
		// Ensure that informers are registered before starting. Coordinated Leader Election leader-elected
		// and may register informer handlers after they are started.
		_ = leaseInformer.Informer()
		_ = lcInformer.Informer()
		//启动协调式 Leader Election 控制器（可选）
		s.GenericAPIServer.AddPostStartHookOrDie("start-kube-apiserver-coordinated-leader-election-controller", func(hookContext genericapiserver.PostStartHookContext) error {
			go leaderelection.RunWithLeaderElection(hookContext, s.GenericAPIServer.LoopbackClientConfig, func() (func(ctx context.Context, workers int), error) {
				controller, err := leaderelection.NewController(
					leaseInformer,
					lcInformer,
					client.CoordinationV1(),
					client.CoordinationV1alpha2(),
				)
				gccontroller := leaderelection.NewLeaseCandidateGC(
					client,
					LeaseCandidateGCPeriod,
					lcInformer,
				)
				return func(ctx context.Context, workers int) {
					go controller.Run(ctx, workers)
					go gccontroller.Run(ctx)
				}, err
			})
			return nil
		})
	}
	//用于支持 Kubernetes 集群间未知版本的服务发现与代理（如 multi-cluster API proxy）。
	if utilfeature.DefaultFeatureGate.Enabled(features.UnknownVersionInteroperabilityProxy) {
		peeraddress := getPeerAddress(c.Extra.PeerAdvertiseAddress, c.Generic.PublicAddress, publicServicePort)
		peerEndpointCtrl := peerreconcilers.New(
			c.Generic.APIServerID,
			peeraddress,
			c.Extra.PeerEndpointLeaseReconciler,
			c.Extra.PeerEndpointReconcileInterval,
			client)
		s.GenericAPIServer.AddPostStartHookOrDie("peer-endpoint-reconciler-controller",
			func(hookContext genericapiserver.PostStartHookContext) error {
				peerEndpointCtrl.Start(hookContext.Done())
				return nil
			})
		s.GenericAPIServer.AddPreShutdownHookOrDie("peer-endpoint-reconciler-controller",
			func() error {
				peerEndpointCtrl.Stop()
				return nil
			})
		if c.Extra.PeerProxy != nil {
			s.GenericAPIServer.AddPostStartHookOrDie("unknown-version-proxy-filter", func(context genericapiserver.PostStartHookContext) error {
				err := c.Extra.PeerProxy.WaitForCacheSync(context.Done())
				return err
			})
		}
	}
	//控制器负责动态维护 clientCA（客户端证书）和 requestHeaderCA 的信任链，并监听其证书变化。
	s.GenericAPIServer.AddPostStartHookOrDie("start-cluster-authentication-info-controller", func(hookContext genericapiserver.PostStartHookContext) error {
		controller := clusterauthenticationtrust.NewClusterAuthenticationTrustController(s.ClusterAuthenticationInfo, client)
		// prime values and start listeners
		if s.ClusterAuthenticationInfo.ClientCA != nil {
			s.ClusterAuthenticationInfo.ClientCA.AddListener(controller)
			if controller, ok := s.ClusterAuthenticationInfo.ClientCA.(dynamiccertificates.ControllerRunner); ok {
				// runonce to be sure that we have a value.
				if err := controller.RunOnce(hookContext); err != nil {
					runtime.HandleError(err)
				}
				go controller.Run(hookContext, 1)
			}
		}
		if s.ClusterAuthenticationInfo.RequestHeaderCA != nil {
			s.ClusterAuthenticationInfo.RequestHeaderCA.AddListener(controller)
			if controller, ok := s.ClusterAuthenticationInfo.RequestHeaderCA.(dynamiccertificates.ControllerRunner); ok {
				// runonce to be sure that we have a value.
				if err := controller.RunOnce(hookContext); err != nil {
					runtime.HandleError(err)
				}
				go controller.Run(hookContext, 1)
			}
		}

		go controller.Run(hookContext, 1)
		return nil
	})
	//该控制器用于注册 apiserver 的身份 lease 对象，供集群中其他组件（如 controller-manager）感知 apiserver 存活状态。
	if utilfeature.DefaultFeatureGate.Enabled(apiserverfeatures.APIServerIdentity) {
		s.GenericAPIServer.AddPostStartHookOrDie("start-kube-apiserver-identity-lease-controller", func(hookContext genericapiserver.PostStartHookContext) error {
			leaseName := s.GenericAPIServer.APIServerID
			holderIdentity := s.GenericAPIServer.APIServerID + "_" + string(uuid.NewUUID())

			peeraddress := getPeerAddress(c.Extra.PeerAdvertiseAddress, c.Generic.PublicAddress, publicServicePort)
			// must replace ':,[]' in [ip:port] to be able to store this as a valid label value
			controller := lease.NewController(
				clock.RealClock{},
				client,
				holderIdentity,
				int32(IdentityLeaseDurationSeconds),
				nil,
				IdentityLeaseRenewIntervalPeriod,
				leaseName,
				metav1.NamespaceSystem,
				// TODO: receive identity label value as a parameter when post start hook is moved to generic apiserver.
				labelAPIServerHeartbeatFunc(name, peeraddress))
			go controller.Run(hookContext)
			return nil
		})
		// TODO: move this into generic apiserver and make the lease identity value configurable
		s.GenericAPIServer.AddPostStartHookOrDie("start-kube-apiserver-identity-lease-garbage-collector", func(hookContext genericapiserver.PostStartHookContext) error {
			go apiserverleasegc.NewAPIServerLeaseGC(
				client,
				IdentityLeaseGCPeriod,
				metav1.NamespaceSystem,
				IdentityLeaseComponentLabelKey+"="+name,
			).Run(hookContext.Done())
			return nil
		})
	}
	//确保后端 watch cache（基于 etcd）初始化完成之后，API Server 才被认为是 ready。
	if utilfeature.DefaultFeatureGate.Enabled(apiserverfeatures.WatchCacheInitializationPostStartHook) {
		s.GenericAPIServer.AddPostStartHookOrDie("storage-readiness", s.GenericAPIServer.StorageReadinessHook.Hook)
	}
	//用于追踪和管理已废弃的 serviceaccount token 等老旧认证凭证。
	s.GenericAPIServer.AddPostStartHookOrDie("start-legacy-token-tracking-controller", func(hookContext genericapiserver.PostStartHookContext) error {
		go legacytokentracking.NewController(client).Run(hookContext.Done())
		return nil
	})

	return s, nil
}

func labelAPIServerHeartbeatFunc(identity string, peeraddress string) lease.ProcessLeaseFunc {
	return func(lease *coordinationapiv1.Lease) error {
		if lease.Labels == nil {
			lease.Labels = map[string]string{}
		}

		if lease.Annotations == nil {
			lease.Annotations = map[string]string{}
		}

		// This label indiciates the identity of the lease object.
		lease.Labels[IdentityLeaseComponentLabelKey] = identity

		hostname, err := os.Hostname()
		if err != nil {
			return err
		}

		// convenience label to easily map a lease object to a specific apiserver
		lease.Labels[apiv1.LabelHostname] = hostname

		// Include apiserver network location <ip_port> used by peers to proxy requests between kube-apiservers
		if utilfeature.DefaultFeatureGate.Enabled(features.UnknownVersionInteroperabilityProxy) {
			if peeraddress != "" {
				lease.Annotations[apiv1.AnnotationPeerAdvertiseAddress] = peeraddress
			}
		}
		return nil
	}
}
