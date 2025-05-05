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

// Package app does all of the work necessary to create a Kubernetes
// APIServer by binding together the API, master and APIServer infrastructure.
// It can be configured and called directly or via the hyperkube framework.
package app

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/spf13/cobra"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/notfoundhandler"
	"k8s.io/apiserver/pkg/util/webhook"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	_ "k8s.io/component-base/metrics/prometheus/workqueue"
	"k8s.io/component-base/term"
	utilversion "k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	"k8s.io/klog/v2"
	aggregatorapiserver "k8s.io/kube-aggregator/pkg/apiserver"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	"k8s.io/kubernetes/pkg/controlplane/reconcilers"
	kubeapiserveradmission "k8s.io/kubernetes/pkg/kubeapiserver/admission"
)

func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}

// NewAPIServerCommand creates a *cobra.Command object with default parameters
func NewAPIServerCommand() *cobra.Command {
	_, featureGate := featuregate.DefaultComponentGlobalsRegistry.ComponentGlobalsOrRegister(
		featuregate.DefaultKubeComponent, utilversion.DefaultBuildEffectiveVersion(), utilfeature.DefaultMutableFeatureGate)
	s := options.NewServerRunOptions()
	//设置带有优雅退出（graceful shutdown）信号处理的上下文（Context）
	ctx := genericapiserver.SetupSignalContext()

	cmd := &cobra.Command{
		Use: "kube-apiserver",
		Long: `The Kubernetes API server validates and configures data
for the api objects which include pods, services, replicationcontrollers, and
others. The API Server services REST operations and provides the frontend to the
cluster's shared state through which all other components interact.`,

		// stop printing usage when the command errors
		SilenceUsage: true,
		//设置注册好的 Feature Gate。
		PersistentPreRunE: func(*cobra.Command, []string) error {
			if err := featuregate.DefaultComponentGlobalsRegistry.Set(); err != nil {
				return err
			}
			//禁用默认的 client-go 警告处理器（因为 kube-apiserver 是 loopback client，自发请求，不需要 warning）。
			// silence client-go warnings.
			// kube-apiserver loopback clients should not log self-issued warnings.
			rest.SetDefaultWarningHandler(rest.NoWarnings{})
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			verflag.PrintAndExitIfRequested()
			fs := cmd.Flags()
			// Activate logging as soon as possible, after that
			// show flags with the final logging configuration.
			//应用日志配置。
			if err := logsapi.ValidateAndApply(s.Logs, featureGate); err != nil {
				return err
			}
			//			//打印命令行 flags。
			cliflag.PrintFlags(fs)

			// set default options
			//设置默认参数。
			completedOptions, err := s.Complete(ctx)
			if err != nil {
				return err
			}

			// validate options
			if errs := completedOptions.Validate(); len(errs) != 0 {
				return utilerrors.NewAggregate(errs)
			}
			// add feature enablement metrics
			featureGate.AddMetrics()
			return Run(ctx, completedOptions)
		},
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}
	cmd.SetContext(ctx)

	fs := cmd.Flags()
	namedFlagSets := s.Flags()
	verflag.AddFlags(namedFlagSets.FlagSet("global"))
	globalflag.AddGlobalFlags(namedFlagSets.FlagSet("global"), cmd.Name(), logs.SkipLoggingConfigurationFlags())
	options.AddCustomGlobalFlags(namedFlagSets.FlagSet("generic"))
	for _, f := range namedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, namedFlagSets, cols)

	return cmd
}

// Run runs the specified APIServer.  This should never exit.
func Run(ctx context.Context, opts options.CompletedOptions) error {
	// To help debugging, immediately log version
	klog.Infof("Version: %+v", utilversion.Get())

	klog.InfoS("Golang settings", "GOGC", os.Getenv("GOGC"), "GOMAXPROCS", os.Getenv("GOMAXPROCS"), "GOTRACEBACK", os.Getenv("GOTRACEBACK"))

	config, err := NewConfig(opts)
	if err != nil {
		return err
	}
	completed, err := config.Complete()
	if err != nil {
		return err
	}
	server, err := CreateServerChain(completed)
	if err != nil {
		return err
	}

	prepared, err := server.PrepareRun()
	if err != nil {
		return err
	}

	return prepared.Run(ctx)
}

// CreateServerChain creates the apiservers connected via delegation.
// 用于构建整个 apiserver 服务器链的核心函数。它负责创建并“串联”三个 apiserver 组件：
// CustomResourceDefinition（CRD）扩展 apiserver
// 核心 Kubernetes apiserver（KubeAPIServer）
// Aggregator apiserver（用于代理/聚合）
func CreateServerChain(config CompletedConfig) (*aggregatorapiserver.APIAggregator, error) {
	notFoundHandler := notfoundhandler.New(config.KubeAPIs.ControlPlane.Generic.Serializer, genericapifilters.NoMuxAndDiscoveryIncompleteKey)
	//创建扩展 APIServer（负责处理自定义资源 CRD），它是整条链中最底层的 server。
	//注意：这里通过 NewEmptyDelegateWithCustomHandler 把 404 handler 传进去。
	apiExtensionsServer, err := config.ApiExtensions.New(genericapiserver.NewEmptyDelegateWithCustomHandler(notFoundHandler))
	if err != nil {
		return nil, err
	}
	//这段代码检查是否开启了对 "customresourcedefinitions" 这个资源的支持。稍后 aggregator server 需要这个信息来决定是否对 CRD 聚合。
	crdAPIEnabled := config.ApiExtensions.GenericConfig.MergedResourceConfig.ResourceEnabled(apiextensionsv1.SchemeGroupVersion.WithResource("customresourcedefinitions"))

	kubeAPIServer, err := config.KubeAPIs.New(apiExtensionsServer.GenericAPIServer)
	if err != nil {
		return nil, err
	}

	// aggregator comes last in the chain
	aggregatorServer, err := controlplaneapiserver.CreateAggregatorServer(config.Aggregator, kubeAPIServer.ControlPlane.GenericAPIServer, apiExtensionsServer.Informers.Apiextensions().V1().CustomResourceDefinitions(), crdAPIEnabled, apiVersionPriorities)
	if err != nil {
		// we don't need special handling for innerStopCh because the aggregator server doesn't create any go routines
		return nil, err
	}

	return aggregatorServer, nil
}

// CreateKubeAPIServerConfig creates all the resources for running the API server, but runs none of them
// 用于构建 kube-apiserver 的配置（Config 对象），但并不会启动任何服务
func CreateKubeAPIServerConfig(
	opts options.CompletedOptions,
	genericConfig *genericapiserver.Config,
	versionedInformers clientgoinformers.SharedInformerFactory,
	storageFactory *serverstorage.DefaultStorageFactory,
) (
	*controlplane.Config,
	aggregatorapiserver.ServiceResolver,
	[]admission.PluginInitializer,
	error,
) {
	// global stuff
	capabilities.Setup(opts.AllowPrivileged, opts.MaxConnectionBytesPerSec)

	// additional admission initializers
	//admission plugin 是处理 API 请求前做的验证、变更、拒绝；
	//admission plugins 可能依赖外部配置，比如 cloud config；
	kubeAdmissionConfig := &kubeapiserveradmission.Config{
		CloudConfigFile: opts.CloudProvider.CloudConfigFile,
	}
	kubeInitializers, err := kubeAdmissionConfig.New()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create admission plugin initializer: %w", err)
	}
	//用于 aggregator（API 聚合器）模式；
	//能将 k8s.io 的 API 请求转发到另一个后端服务；
	//需要将服务名解析成 IP/端口。
	serviceResolver := buildServiceResolver(opts.EnableAggregatorRouting, genericConfig.LoopbackClientConfig.Host, versionedInformers)
	//将所有参数整合并构造出 controlplane 配置；
	//controlplaneapiserver.CreateConfig 是 kube-apiserver 构造核心部分的关键步骤；
	controlplaneConfig, admissionInitializers, err := controlplaneapiserver.CreateConfig(opts.CompletedOptions, genericConfig, versionedInformers, storageFactory, serviceResolver, kubeInitializers)
	if err != nil {
		return nil, nil, nil, err
	}

	config := &controlplane.Config{
		ControlPlane: *controlplaneConfig,
		Extra: controlplane.Extra{
			KubeletClientConfig: opts.KubeletConfig,

			ServiceIPRange:          opts.PrimaryServiceClusterIPRange,
			APIServerServiceIP:      opts.APIServerServiceIP,
			SecondaryServiceIPRange: opts.SecondaryServiceClusterIPRange,

			APIServerServicePort: 443,

			ServiceNodePortRange:      opts.ServiceNodePortRange,
			KubernetesServiceNodePort: opts.KubernetesServiceNodePort,

			EndpointReconcilerType: reconcilers.Type(opts.EndpointReconcilerType),
			MasterCount:            opts.MasterCount,
		},
	}
	//用于指定访问某些组件（如 kubelet、aggregated API server）时的网络策略；
	//
	//如果配置了 EgressSelector，则使用它来构造 kubelet dialer；
	//
	//同时为 /proxy/... API 子资源设置自定义的 Transport；
	if config.ControlPlane.Generic.EgressSelector != nil {
		// Use the config.ControlPlane.Generic.EgressSelector lookup to find the dialer to connect to the kubelet
		config.Extra.KubeletClientConfig.Lookup = config.ControlPlane.Generic.EgressSelector.Lookup

		// Use the config.ControlPlane.Generic.EgressSelector lookup as the transport used by the "proxy" subresources.
		networkContext := egressselector.Cluster.AsNetworkContext()
		dialer, err := config.ControlPlane.Generic.EgressSelector.Lookup(networkContext)
		if err != nil {
			return nil, nil, nil, err
		}
		c := config.ControlPlane.Extra.ProxyTransport.Clone()
		c.DialContext = dialer
		config.ControlPlane.ProxyTransport = c
	}

	return config, serviceResolver, admissionInitializers, nil
}

var testServiceResolver webhook.ServiceResolver

// SetServiceResolverForTests allows the service resolver to be overridden during tests.
// Tests using this function must run serially as this function is not safe to call concurrently with server start.
func SetServiceResolverForTests(resolver webhook.ServiceResolver) func() {
	if testServiceResolver != nil {
		panic("test service resolver is set: tests are either running concurrently or clean up was skipped")
	}

	testServiceResolver = resolver

	return func() {
		testServiceResolver = nil
	}
}

func buildServiceResolver(enabledAggregatorRouting bool, hostname string, informer clientgoinformers.SharedInformerFactory) webhook.ServiceResolver {
	if testServiceResolver != nil {
		return testServiceResolver
	}

	var serviceResolver webhook.ServiceResolver
	if enabledAggregatorRouting {
		serviceResolver = aggregatorapiserver.NewEndpointServiceResolver(
			informer.Core().V1().Services().Lister(),
			informer.Core().V1().Endpoints().Lister(),
		)
	} else {
		serviceResolver = aggregatorapiserver.NewClusterIPServiceResolver(
			informer.Core().V1().Services().Lister(),
		)
	}

	// resolve kubernetes.default.svc locally
	if localHost, err := url.Parse(hostname); err == nil {
		serviceResolver = aggregatorapiserver.NewLoopbackServiceResolver(serviceResolver, localHost)
	}
	return serviceResolver
}
