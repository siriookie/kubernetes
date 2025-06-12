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

package options

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	cliflag "k8s.io/component-base/cli/flag"
	componentbaseconfig "k8s.io/component-base/config"
	"k8s.io/component-base/config/options"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics"
	utilversion "k8s.io/component-base/version"
	"k8s.io/klog/v2"
	schedulerappconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	netutils "k8s.io/utils/net"
)

// Options has all the params needed to run a Scheduler
type Options struct {
	// The default values.
	ComponentConfig *kubeschedulerconfig.KubeSchedulerConfiguration

	SecureServing  *apiserveroptions.SecureServingOptionsWithLoopback
	Authentication *apiserveroptions.DelegatingAuthenticationOptions
	Authorization  *apiserveroptions.DelegatingAuthorizationOptions
	Metrics        *metrics.Options
	Logs           *logs.Options
	Deprecated     *DeprecatedOptions
	LeaderElection *componentbaseconfig.LeaderElectionConfiguration

	// ConfigFile is the location of the scheduler server's configuration file.
	ConfigFile string

	// WriteConfigTo is the path where the default configuration will be written.
	WriteConfigTo string

	Master string

	// ComponentGlobalsRegistry is the registry where the effective versions and feature gates for all components are stored.
	ComponentGlobalsRegistry featuregate.ComponentGlobalsRegistry

	// Flags hold the parsed CLI flags.
	Flags *cliflag.NamedFlagSets
}

// NewOptions returns default scheduler app options.
func NewOptions() *Options {
	// make sure DefaultKubeComponent is registered in the DefaultComponentGlobalsRegistry.
	if featuregate.DefaultComponentGlobalsRegistry.EffectiveVersionFor(featuregate.DefaultKubeComponent) == nil {
		featureGate := utilfeature.DefaultMutableFeatureGate
		effectiveVersion := utilversion.DefaultKubeEffectiveVersion()
		utilruntime.Must(featuregate.DefaultComponentGlobalsRegistry.Register(featuregate.DefaultKubeComponent, effectiveVersion, featureGate))
	}
	o := &Options{
		SecureServing:  apiserveroptions.NewSecureServingOptions().WithLoopback(),
		Authentication: apiserveroptions.NewDelegatingAuthenticationOptions(),
		Authorization:  apiserveroptions.NewDelegatingAuthorizationOptions(),
		Deprecated: &DeprecatedOptions{
			PodMaxInUnschedulablePodsDuration: 5 * time.Minute,
		},
		LeaderElection: &componentbaseconfig.LeaderElectionConfiguration{
			LeaderElect:       true,
			LeaseDuration:     metav1.Duration{Duration: 15 * time.Second},
			RenewDeadline:     metav1.Duration{Duration: 10 * time.Second},
			RetryPeriod:       metav1.Duration{Duration: 2 * time.Second},
			ResourceLock:      "leases",
			ResourceName:      "kube-scheduler",
			ResourceNamespace: "kube-system",
		},
		Metrics:                  metrics.NewOptions(),
		Logs:                     logs.NewOptions(),
		ComponentGlobalsRegistry: featuregate.DefaultComponentGlobalsRegistry,
	}

	o.Authentication.TolerateInClusterLookupFailure = true
	o.Authentication.RemoteKubeConfigFileOptional = true
	o.Authorization.RemoteKubeConfigFileOptional = true

	// Set the PairName but leave certificate directory blank to generate in-memory by default
	o.SecureServing.ServerCert.CertDirectory = ""
	o.SecureServing.ServerCert.PairName = "kube-scheduler"
	o.SecureServing.BindPort = kubeschedulerconfig.DefaultKubeSchedulerPort

	o.initFlags()

	return o
}

// ApplyDeprecated obtains the deprecated CLI args and set them to `o.ComponentConfig` if specified.
func (o *Options) ApplyDeprecated() {
	if o.Flags == nil {
		return
	}
	// Obtain deprecated CLI args. Set them to cfg if specified in command line.
	deprecated := o.Flags.FlagSet("deprecated")
	if deprecated.Changed("profiling") {
		o.ComponentConfig.EnableProfiling = o.Deprecated.EnableProfiling
	}
	if deprecated.Changed("contention-profiling") {
		o.ComponentConfig.EnableContentionProfiling = o.Deprecated.EnableContentionProfiling
	}
	if deprecated.Changed("kubeconfig") {
		o.ComponentConfig.ClientConnection.Kubeconfig = o.Deprecated.Kubeconfig
	}
	if deprecated.Changed("kube-api-content-type") {
		o.ComponentConfig.ClientConnection.ContentType = o.Deprecated.ContentType
	}
	if deprecated.Changed("kube-api-qps") {
		o.ComponentConfig.ClientConnection.QPS = o.Deprecated.QPS
	}
	if deprecated.Changed("kube-api-burst") {
		o.ComponentConfig.ClientConnection.Burst = o.Deprecated.Burst
	}
}

// ApplyLeaderElectionTo obtains the CLI args related with leaderelection, and override the values in `cfg`.
// Then the `cfg` object is injected into the `options` object.
func (o *Options) ApplyLeaderElectionTo(cfg *kubeschedulerconfig.KubeSchedulerConfiguration) {
	if o.Flags == nil {
		return
	}
	// Obtain CLI args related with leaderelection. Set them to `cfg` if specified in command line.
	leaderelection := o.Flags.FlagSet("leader election")
	//逐个检查是否有命令行参数被用户显式修改过
	//比如 --leader-elect 是否被命令行传入，如果传入了，就用 o.LeaderElection.LeaderElect 这个值覆盖 cfg.LeaderElection.LeaderElect。
	if leaderelection.Changed("leader-elect") {
		cfg.LeaderElection.LeaderElect = o.LeaderElection.LeaderElect
	}
	if leaderelection.Changed("leader-elect-lease-duration") {
		cfg.LeaderElection.LeaseDuration = o.LeaderElection.LeaseDuration
	}
	if leaderelection.Changed("leader-elect-renew-deadline") {
		cfg.LeaderElection.RenewDeadline = o.LeaderElection.RenewDeadline
	}
	if leaderelection.Changed("leader-elect-retry-period") {
		cfg.LeaderElection.RetryPeriod = o.LeaderElection.RetryPeriod
	}
	if leaderelection.Changed("leader-elect-resource-lock") {
		cfg.LeaderElection.ResourceLock = o.LeaderElection.ResourceLock
	}
	if leaderelection.Changed("leader-elect-resource-name") {
		cfg.LeaderElection.ResourceName = o.LeaderElection.ResourceName
	}
	if leaderelection.Changed("leader-elect-resource-namespace") {
		cfg.LeaderElection.ResourceNamespace = o.LeaderElection.ResourceNamespace
	}

	o.ComponentConfig = cfg
}

// initFlags initializes flags by section name.
func (o *Options) initFlags() {
	if o.Flags != nil {
		return
	}

	nfs := cliflag.NamedFlagSets{}
	fs := nfs.FlagSet("misc")
	fs.StringVar(&o.ConfigFile, "config", o.ConfigFile, "The path to the configuration file.")
	fs.StringVar(&o.WriteConfigTo, "write-config-to", o.WriteConfigTo, "If set, write the configuration values to this file and exit.")
	fs.StringVar(&o.Master, "master", o.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig)")

	o.SecureServing.AddFlags(nfs.FlagSet("secure serving"))
	o.Authentication.AddFlags(nfs.FlagSet("authentication"))
	o.Authorization.AddFlags(nfs.FlagSet("authorization"))
	o.Deprecated.AddFlags(nfs.FlagSet("deprecated"))
	options.BindLeaderElectionFlags(o.LeaderElection, nfs.FlagSet("leader election"))
	o.ComponentGlobalsRegistry.AddFlags(nfs.FlagSet("feature gate"))
	o.Metrics.AddFlags(nfs.FlagSet("metrics"))
	logsapi.AddFlags(o.Logs, nfs.FlagSet("logs"))

	o.Flags = &nfs
}

// ApplyTo applies the scheduler options to the given scheduler app configuration.
// 将 Options 中的配置应用（赋值和初始化）到 调度器的最终运行配置结构体（schedulerappconfig.Config）中，准备让调度器程序真正使用这些配置启动和运行。
func (o *Options) ApplyTo(logger klog.Logger, c *schedulerappconfig.Config) error {
	if err := o.ComponentGlobalsRegistry.SetFallback(); err != nil {
		return err
	}
	if len(o.ConfigFile) == 0 {
		// If the --config arg is not specified, honor the deprecated as well as leader election CLI args.
		// 没有指定配置文件，使用命令行旧参数（deprecated）和 leader election 配置
		o.ApplyDeprecated()
		o.ApplyLeaderElectionTo(o.ComponentConfig)
		c.ComponentConfig = *o.ComponentConfig
	} else {
		cfg, err := LoadConfigFromFile(logger, o.ConfigFile)
		if err != nil {
			return err
		}
		// If the --config arg is specified, honor the leader election CLI args only.
		o.ApplyLeaderElectionTo(cfg)

		if err := validation.ValidateKubeSchedulerConfiguration(cfg); err != nil {
			return err
		}

		c.ComponentConfig = *cfg
	}

	// Build kubeconfig first to so that if it fails, it doesn't cause leaking
	// goroutines (started from initializing secure serving - which underneath
	// creates a queue which in its constructor starts a goroutine).
	kubeConfig, err := createKubeConfig(c.ComponentConfig.ClientConnection, o.Master)
	if err != nil {
		return err
	}
	c.KubeConfig = kubeConfig

	if err := o.SecureServing.ApplyTo(&c.SecureServing, &c.LoopbackClientConfig); err != nil {
		return err
	}
	if o.SecureServing != nil && (o.SecureServing.BindPort != 0 || o.SecureServing.Listener != nil) {
		if err := o.Authentication.ApplyTo(&c.Authentication, c.SecureServing, nil); err != nil {
			return err
		}
		if err := o.Authorization.ApplyTo(&c.Authorization); err != nil {
			return err
		}
	}
	o.Metrics.Apply()

	// Apply value independently instead of using ApplyDeprecated() because it can't be configured via ComponentConfig.
	if o.Deprecated != nil {
		c.PodMaxInUnschedulablePodsDuration = o.Deprecated.PodMaxInUnschedulablePodsDuration
	}

	return nil
}

// Validate validates all the required options.
// 在 kube-scheduler 启动之前，会调用这个函数对各种配置项（如认证、授权、指标、API 配置等）做检查，确保不会因为参数问题导致服务启动失败。
func (o *Options) Validate() []error {
	var errs []error
	if err := o.ComponentGlobalsRegistry.SetFallback(); err != nil {
		errs = append(errs, err)
	} else {
		errs = append(errs, o.ComponentGlobalsRegistry.Validate()...)
	}
	//检查 KubeSchedulerConfiguration（比如是否有多个 Profile、百分比是否合法、插件设置等）
	if err := validation.ValidateKubeSchedulerConfiguration(o.ComponentConfig); err != nil {
		errs = append(errs, err.Errors()...)
	}
	errs = append(errs, o.SecureServing.Validate()...)  // HTTPS 服务配置是否完整
	errs = append(errs, o.Authentication.Validate()...) // 认证配置是否合法
	errs = append(errs, o.Authorization.Validate()...)  // 授权配置是否合法
	errs = append(errs, o.Metrics.Validate()...)        // 指标暴露配置是否合法
	// 验证调度器支持的版本号是否合理
	effectiveVersion := o.ComponentGlobalsRegistry.EffectiveVersionFor(featuregate.DefaultKubeComponent)
	if err := utilversion.ValidateKubeEffectiveVersion(effectiveVersion); err != nil {
		errs = append(errs, err)
	}

	return errs
}

// Config return a scheduler config object
// 构造 kube-scheduler 的完整运行配置，包括证书、客户端、事件系统、informer、leader election 等。
func (o *Options) Config(ctx context.Context) (*schedulerappconfig.Config, error) {
	logger := klog.FromContext(ctx)
	if o.SecureServing != nil {
		if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")}); err != nil {
			return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
		}
	}

	c := &schedulerappconfig.Config{}
	if err := o.ApplyTo(logger, c); err != nil {
		return nil, err
	}

	// Prepare kube clients.
	client, eventClient, err := createClients(c.KubeConfig)
	if err != nil {
		return nil, err
	}
	//用于发出调度器行为相关的事件（比如调度成功、失败等）。

	c.EventBroadcaster = events.NewEventBroadcasterAdapterWithContext(ctx, eventClient)

	// Set up leader election if enabled.
	var leaderElectionConfig *leaderelection.LeaderElectionConfig
	//如果启用了 HA 模式，调度器需要竞选“主节点”来独占调度任务。
	//构造 LeaderElectionConfig 来配置竞选相关参数。
	if c.ComponentConfig.LeaderElection.LeaderElect {
		// Use the scheduler name in the first profile to record leader election.
		schedulerName := corev1.DefaultSchedulerName
		if len(c.ComponentConfig.Profiles) != 0 {
			schedulerName = c.ComponentConfig.Profiles[0].SchedulerName
		}
		coreRecorder := c.EventBroadcaster.DeprecatedNewLegacyRecorder(schedulerName)
		leaderElectionConfig, err = makeLeaderElectionConfig(c.ComponentConfig.LeaderElection, c.KubeConfig, coreRecorder)
		if err != nil {
			return nil, err
		}
	}

	c.Client = client
	c.InformerFactory = scheduler.NewInformerFactory(client, 0)
	dynClient := dynamic.NewForConfigOrDie(c.KubeConfig)
	c.DynInformerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynClient, 0, corev1.NamespaceAll, nil)
	c.LeaderElection = leaderElectionConfig

	return c, nil
}

// makeLeaderElectionConfig builds a leader election configuration. It will
// create a new resource lock associated with the configuration.
func makeLeaderElectionConfig(config componentbaseconfig.LeaderElectionConfiguration, kubeConfig *restclient.Config, recorder record.EventRecorder) (*leaderelection.LeaderElectionConfig, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("unable to get hostname: %v", err)
	}
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	id := hostname + "_" + string(uuid.NewUUID())

	rl, err := resourcelock.NewFromKubeconfig(config.ResourceLock,
		config.ResourceNamespace,
		config.ResourceName,
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		},
		kubeConfig,
		config.RenewDeadline.Duration)
	if err != nil {
		return nil, fmt.Errorf("couldn't create resource lock: %v", err)
	}

	return &leaderelection.LeaderElectionConfig{
		Lock:            rl,
		LeaseDuration:   config.LeaseDuration.Duration,
		RenewDeadline:   config.RenewDeadline.Duration,
		RetryPeriod:     config.RetryPeriod.Duration,
		WatchDog:        leaderelection.NewLeaderHealthzAdaptor(time.Second * 20),
		Name:            "kube-scheduler",
		ReleaseOnCancel: true,
	}, nil
}

// createKubeConfig creates a kubeConfig from the given config and masterOverride.
// TODO remove masterOverride when CLI flags are removed.
func createKubeConfig(config componentbaseconfig.ClientConnectionConfiguration, masterOverride string) (*restclient.Config, error) {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(masterOverride, config.Kubeconfig)
	if err != nil {
		return nil, err
	}

	kubeConfig.DisableCompression = true
	kubeConfig.AcceptContentTypes = config.AcceptContentTypes
	kubeConfig.ContentType = config.ContentType
	kubeConfig.QPS = config.QPS
	kubeConfig.Burst = int(config.Burst)

	return kubeConfig, nil
}

// createClients creates a kube client and an event client from the given kubeConfig
func createClients(kubeConfig *restclient.Config) (clientset.Interface, clientset.Interface, error) {
	client, err := clientset.NewForConfig(restclient.AddUserAgent(kubeConfig, "scheduler"))
	if err != nil {
		return nil, nil, err
	}

	eventClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		return nil, nil, err
	}

	return client, eventClient, nil
}
