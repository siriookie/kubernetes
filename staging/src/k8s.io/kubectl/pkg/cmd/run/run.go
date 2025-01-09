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

package run

import (
	"context"
	"fmt"
	"time"

	"github.com/distribution/reference"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/kubectl/pkg/cmd/attach"
	cmddelete "k8s.io/kubectl/pkg/cmd/delete"
	"k8s.io/kubectl/pkg/cmd/exec"
	"k8s.io/kubectl/pkg/cmd/logs"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/generate"
	generateversioned "k8s.io/kubectl/pkg/generate/versioned"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/interrupt"
	"k8s.io/kubectl/pkg/util/templates"
	uexec "k8s.io/utils/exec"
)

var (
	runLong = templates.LongDesc(i18n.T(`Create and run a particular image in a pod.`))

	runExample = templates.Examples(i18n.T(`
		# Start a nginx pod
		kubectl run nginx --image=nginx

		# Start a hazelcast pod and let the container expose port 5701
		kubectl run hazelcast --image=hazelcast/hazelcast --port=5701

		# Start a hazelcast pod and set environment variables "DNS_DOMAIN=cluster" and "POD_NAMESPACE=default" in the container
		kubectl run hazelcast --image=hazelcast/hazelcast --env="DNS_DOMAIN=cluster" --env="POD_NAMESPACE=default"

		# Start a hazelcast pod and set labels "app=hazelcast" and "env=prod" in the container
		kubectl run hazelcast --image=hazelcast/hazelcast --labels="app=hazelcast,env=prod"

		# Dry run; print the corresponding API objects without creating them
		kubectl run nginx --image=nginx --dry-run=client

		# Start a nginx pod, but overload the spec with a partial set of values parsed from JSON
		kubectl run nginx --image=nginx --overrides='{ "apiVersion": "v1", "spec": { ... } }'

		# Start a busybox pod and keep it in the foreground, don't restart it if it exits
		kubectl run -i -t busybox --image=busybox --restart=Never

		# Start the nginx pod using the default command, but use custom arguments (arg1 .. argN) for that command
		kubectl run nginx --image=nginx -- <arg1> <arg2> ... <argN>

		# Start the nginx pod using a different command and custom arguments
		kubectl run nginx --image=nginx --command -- <cmd> <arg1> ... <argN>`))
)

const (
	defaultPodAttachTimeout = 60 * time.Second
)

var metadataAccessor = meta.NewAccessor()

type RunObject struct {
	Object  runtime.Object
	Mapping *meta.RESTMapping
}

type RunOptions struct {
	cmdutil.OverrideOptions

	PrintFlags  *genericclioptions.PrintFlags
	RecordFlags *genericclioptions.RecordFlags

	DeleteFlags   *cmddelete.DeleteFlags
	DeleteOptions *cmddelete.DeleteOptions

	DryRunStrategy cmdutil.DryRunStrategy

	PrintObj func(runtime.Object) error
	Recorder genericclioptions.Recorder

	ArgsLenAtDash  int
	Attach         bool
	Expose         bool
	Image          string
	Interactive    bool
	LeaveStdinOpen bool
	Port           string
	Privileged     bool
	Quiet          bool
	TTY            bool
	fieldManager   string

	Namespace        string
	EnforceNamespace bool

	genericiooptions.IOStreams
}

func NewRunOptions(streams genericiooptions.IOStreams) *RunOptions {
	return &RunOptions{
		PrintFlags:  genericclioptions.NewPrintFlags("created").WithTypeSetter(scheme.Scheme),
		DeleteFlags: cmddelete.NewDeleteFlags("to use to replace the resource."),
		RecordFlags: genericclioptions.NewRecordFlags(),

		Recorder: genericclioptions.NoopRecorder{},

		IOStreams: streams,
	}
}

func NewCmdRun(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := NewRunOptions(streams)

	cmd := &cobra.Command{
		Use:                   "run NAME --image=image [--env=\"key=value\"] [--port=port] [--dry-run=server|client] [--overrides=inline-json] [--command] -- [COMMAND] [args...]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Run a particular image on the cluster"),
		Long:                  runLong,
		Example:               runExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd))
			cmdutil.CheckErr(o.Run(f, cmd, args))
		},
	}

	o.DeleteFlags.AddFlags(cmd)
	o.PrintFlags.AddFlags(cmd)
	o.RecordFlags.AddFlags(cmd)

	addRunFlags(cmd, o)
	cmdutil.AddApplyAnnotationFlags(cmd)
	cmdutil.AddPodRunningTimeoutFlag(cmd, defaultPodAttachTimeout)

	return cmd
}

func addRunFlags(cmd *cobra.Command, opt *RunOptions) {
	cmdutil.AddDryRunFlag(cmd)
	cmd.Flags().StringArray("annotations", []string{}, i18n.T("Annotations to apply to the pod."))
	cmd.Flags().StringVar(&opt.Image, "image", opt.Image, i18n.T("The image for the container to run."))
	cmd.MarkFlagRequired("image")
	cmd.Flags().String("image-pull-policy", "", i18n.T("The image pull policy for the container.  If left empty, this value will not be specified by the client and defaulted by the server."))
	cmd.Flags().Bool("rm", false, "If true, delete the pod after it exits.  Only valid when attaching to the container, e.g. with '--attach' or with '-i/--stdin'.")
	cmd.Flags().StringArray("env", []string{}, "Environment variables to set in the container.")
	cmd.Flags().StringVar(&opt.Port, "port", opt.Port, i18n.T("The port that this container exposes."))
	cmd.Flags().StringP("labels", "l", "", "Comma separated labels to apply to the pod. Will override previous values.")
	cmd.Flags().BoolVarP(&opt.Interactive, "stdin", "i", opt.Interactive, "Keep stdin open on the container in the pod, even if nothing is attached.")
	cmd.Flags().BoolVarP(&opt.TTY, "tty", "t", opt.TTY, "Allocate a TTY for the container in the pod.")
	cmd.Flags().BoolVar(&opt.Attach, "attach", opt.Attach, "If true, wait for the Pod to start running, and then attach to the Pod as if 'kubectl attach ...' were called.  Default false, unless '-i/--stdin' is set, in which case the default is true. With '--restart=Never' the exit code of the container process is returned.")
	cmd.Flags().BoolVar(&opt.LeaveStdinOpen, "leave-stdin-open", opt.LeaveStdinOpen, "If the pod is started in interactive mode or with stdin, leave stdin open after the first attach completes. By default, stdin will be closed after the first attach completes.")
	cmd.Flags().String("restart", "Always", i18n.T("The restart policy for this Pod.  Legal values [Always, OnFailure, Never]."))
	cmd.Flags().Bool("command", false, "If true and extra arguments are present, use them as the 'command' field in the container, rather than the 'args' field which is the default.")
	cmd.Flags().BoolVar(&opt.Expose, "expose", opt.Expose, "If true, create a ClusterIP service associated with the pod.  Requires `--port`.")
	cmd.Flags().BoolVarP(&opt.Quiet, "quiet", "q", opt.Quiet, "If true, suppress prompt messages.")
	cmd.Flags().BoolVar(&opt.Privileged, "privileged", opt.Privileged, i18n.T("If true, run the container in privileged mode."))
	cmdutil.AddFieldManagerFlagVar(cmd, &opt.fieldManager, "kubectl-run")
	opt.AddOverrideFlags(cmd)
}

func (o *RunOptions) Complete(f cmdutil.Factory, cmd *cobra.Command) error {
	var err error

	o.RecordFlags.Complete(cmd)
	o.Recorder, err = o.RecordFlags.ToRecorder()
	if err != nil {
		return err
	}

	o.ArgsLenAtDash = cmd.ArgsLenAtDash()
	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}
	dynamicClient, err := f.DynamicClient()
	if err != nil {
		return err
	}

	attachFlag := cmd.Flags().Lookup("attach")
	if !attachFlag.Changed && o.Interactive {
		o.Attach = true
	}

	cmdutil.PrintFlagsWithDryRunStrategy(o.PrintFlags, o.DryRunStrategy)
	printer, err := o.PrintFlags.ToPrinter()
	if err != nil {
		return err
	}
	o.PrintObj = func(obj runtime.Object) error {
		return printer.PrintObj(obj, o.Out)
	}

	deleteOpts, err := o.DeleteFlags.ToOptions(dynamicClient, o.IOStreams)
	if err != nil {
		return err
	}

	deleteOpts.IgnoreNotFound = true
	deleteOpts.WaitForDeletion = false
	deleteOpts.GracePeriod = -1
	deleteOpts.Quiet = o.Quiet

	o.DeleteOptions = deleteOpts

	return nil
}

// kubectl run 是用来快速创建一个 Pod 的命令，提供运行容器的基本能力
// kubectl run my-nginx --image=nginx:1.21
func (o *RunOptions) Run(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	// Let kubectl run follow rules for `--`, see #13004 issue
	if len(args) == 0 || o.ArgsLenAtDash == 0 {
		return cmdutil.UsageErrorf(cmd, "NAME is required for run")
	}

	timeout, err := cmdutil.GetPodRunningTimeoutFlag(cmd)
	if err != nil {
		return cmdutil.UsageErrorf(cmd, "%v", err)
	}

	// validate image name
	//--image 是必填参数。如果未提供或格式错误，直接返回错误
	if o.Image == "" {
		return fmt.Errorf("--image is required")
	}

	if !reference.ReferenceRegexp.MatchString(o.Image) {
		return fmt.Errorf("Invalid image name %q: %v", o.Image, reference.ErrReferenceInvalidFormat)
	}
	//校验 -t/--tty 和 -i/--stdin 参数是否搭配正确。
	if o.TTY && !o.Interactive {
		return cmdutil.UsageErrorf(cmd, "-i/--stdin is required for containers with -t/--tty=true")
	}
	// 如果启用了 --expose 参数，则必须提供 --port。
	if o.Expose && len(o.Port) == 0 {
		return cmdutil.UsageErrorf(cmd, "--port must be set when exposing a service")
	}

	o.Namespace, o.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}
	// 设置restart policy
	restartPolicy, err := getRestartPolicy(cmd, o.Interactive)
	if err != nil {
		return err
	}
	//检查 --rm 和 --attach 参数的搭配是否合理。
	remove := cmdutil.GetFlagBool(cmd, "rm")
	if !o.Attach && remove {
		return cmdutil.UsageErrorf(cmd, "--rm should only be used for attached containers")
	}

	if o.Attach && o.DryRunStrategy != cmdutil.DryRunNone {
		return cmdutil.UsageErrorf(cmd, "--dry-run=[server|client] can't be used with attached containers options (--attach, --stdin, or --tty)")
	}

	if err := verifyImagePullPolicy(cmd); err != nil {
		return err
	}
	//这段代码从生成器中获取了一个默认的 Pod 生成器（RunPodV1GeneratorName），
	//生成器会根据用户的输入参数生成一个 Pod 的定义。
	generators := generateversioned.GeneratorFn("run")
	generator, found := generators[generateversioned.RunPodV1GeneratorName]
	if !found {
		return cmdutil.UsageErrorf(cmd, "generator %q not found", generateversioned.RunPodV1GeneratorName)
	}

	names := generator.ParamNames()
	// 填充pod的信息
	params := generate.MakeParams(cmd, names)
	params["name"] = args[0]
	if len(args) > 1 {
		params["args"] = args[1:]
	}

	params["annotations"] = cmdutil.GetFlagStringArray(cmd, "annotations")
	params["env"] = cmdutil.GetFlagStringArray(cmd, "env")

	var createdObjects = []*RunObject{}
	// 实际调用 createGeneratedObject 方法，生成并应用 Pod 对象。
	runObject, err := o.createGeneratedObject(f, cmd, generator, names, params, o.NewOverrider(&corev1.Pod{}))
	if err != nil {
		return err
	}
	createdObjects = append(createdObjects, runObject)

	allErrs := []error{}
	if o.Expose {
		//如果传入了 --expose 参数，则生成并应用对应的 Service 资源
		serviceRunObject, err := o.generateService(f, cmd, params)
		if err != nil {
			allErrs = append(allErrs, err)
		} else {
			createdObjects = append(createdObjects, serviceRunObject)
		}
	}
	// 如果指定了 --attach 参数，kubectl 会等待 Pod 启动，并将用户的输入/输出连接到容器：
	if o.Attach {
		if remove {
			defer o.removeCreatedObjects(f, createdObjects)
		}

		opts := &attach.AttachOptions{
			StreamOptions: exec.StreamOptions{
				IOStreams: o.IOStreams,
				Stdin:     o.Interactive,
				TTY:       o.TTY,
				Quiet:     o.Quiet,
			},
			GetPodTimeout: timeout,
			CommandName:   cmd.Parent().CommandPath() + " attach",

			Attach: &attach.DefaultRemoteAttach{},
		}
		config, err := f.ToRESTConfig()
		if err != nil {
			return err
		}
		opts.Config = config
		opts.AttachFunc = attach.DefaultAttachFunc

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			return err
		}

		attachablePod, err := polymorphichelpers.AttachablePodForObjectFn(f, runObject.Object, opts.GetPodTimeout)
		if err != nil {
			return err
		}
		err = handleAttachPod(f, clientset.CoreV1(), attachablePod.Namespace, attachablePod.Name, opts)
		if err != nil {
			return err
		}

		var pod *corev1.Pod
		waitForExitCode := !o.LeaveStdinOpen && (restartPolicy == corev1.RestartPolicyNever || restartPolicy == corev1.RestartPolicyOnFailure)
		if waitForExitCode {
			// we need different exit condition depending on restart policy
			// for Never it can either fail or succeed, for OnFailure only
			// success matters
			exitCondition := podCompleted
			if restartPolicy == corev1.RestartPolicyOnFailure {
				exitCondition = podSucceeded
			}
			// 等待 Pod 就绪： 在 attach 之前，它会根据 restartPolicy 等配置，等待 Pod 进入正确的运行状态（比如启动成功或者失败）。
			pod, err = waitForPod(clientset.CoreV1(), attachablePod.Namespace, attachablePod.Name, opts.GetPodTimeout, exitCondition)
			if err != nil {
				return err
			}
		} else {
			// after removal is done, return successfully if we are not interested in the exit code
			return nil
		}

		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return nil
		case corev1.PodFailed:
			unknownRcErr := fmt.Errorf("pod %s/%s failed with unknown exit code", pod.Namespace, pod.Name)
			if len(pod.Status.ContainerStatuses) == 0 || pod.Status.ContainerStatuses[0].State.Terminated == nil {
				return unknownRcErr
			}
			// assume here that we have at most one status because kubectl-run only creates one container per pod
			rc := pod.Status.ContainerStatuses[0].State.Terminated.ExitCode
			if rc == 0 {
				return unknownRcErr
			}
			return uexec.CodeExitError{
				Err:  fmt.Errorf("pod %s/%s terminated (%s)\n%s", pod.Namespace, pod.Name, pod.Status.ContainerStatuses[0].State.Terminated.Reason, pod.Status.ContainerStatuses[0].State.Terminated.Message),
				Code: int(rc),
			}
		default:
			return fmt.Errorf("pod %s/%s left in phase %s", pod.Namespace, pod.Name, pod.Status.Phase)
		}

	}
	if runObject != nil {
		if err := o.PrintObj(runObject.Object); err != nil {
			return err
		}
	}

	return utilerrors.NewAggregate(allErrs)
}

func (o *RunOptions) removeCreatedObjects(f cmdutil.Factory, createdObjects []*RunObject) error {
	for _, obj := range createdObjects {
		namespace, err := metadataAccessor.Namespace(obj.Object)
		if err != nil {
			return err
		}
		var name string
		name, err = metadataAccessor.Name(obj.Object)
		if err != nil {
			return err
		}
		r := f.NewBuilder().
			WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
			ContinueOnError().
			NamespaceParam(namespace).DefaultNamespace().
			ResourceNames(obj.Mapping.Resource.Resource+"."+obj.Mapping.Resource.Group, name).
			Flatten().
			Do()
		if err := o.DeleteOptions.DeleteResult(r); err != nil {
			return err
		}
	}

	return nil
}

// waitForPod watches the given pod until the exitCondition is true
func waitForPod(podClient corev1client.PodsGetter, ns, name string, timeout time.Duration, exitCondition watchtools.ConditionFunc) (*corev1.Pod, error) {
	ctx, cancel := watchtools.ContextWithOptionalTimeout(context.Background(), timeout)
	defer cancel()

	fieldSelector := fields.OneTermEqualSelector("metadata.name", name).String()
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fieldSelector
			return podClient.Pods(ns).List(context.TODO(), options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = fieldSelector
			return podClient.Pods(ns).Watch(context.TODO(), options)
		},
	}

	intr := interrupt.New(nil, cancel)
	var result *corev1.Pod
	err := intr.Run(func() error {
		ev, err := watchtools.UntilWithSync(ctx, lw, &corev1.Pod{}, nil, exitCondition)
		if ev != nil {
			result = ev.Object.(*corev1.Pod)
		}
		return err
	})

	return result, err
}

func handleAttachPod(f cmdutil.Factory, podClient corev1client.PodsGetter, ns, name string, opts *attach.AttachOptions) error {
	pod, err := waitForPod(podClient, ns, name, opts.GetPodTimeout, podRunningAndReady)
	if err != nil && err != ErrPodCompleted {
		return err
	}

	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return logOpts(f, pod, opts)
	}

	opts.Pod = pod
	opts.PodName = name
	opts.Namespace = ns

	if opts.AttachFunc == nil {
		opts.AttachFunc = attach.DefaultAttachFunc
	}

	if err := opts.Run(); err != nil {
		fmt.Fprintf(opts.ErrOut, "warning: couldn't attach to pod/%s, falling back to streaming logs: %v\n", name, err)
		return logOpts(f, pod, opts)
	}
	return nil
}

// logOpts logs output from opts to the pods log.
func logOpts(restClientGetter genericclioptions.RESTClientGetter, pod *corev1.Pod, opts *attach.AttachOptions) error {
	ctrName, err := opts.GetContainerName(pod)
	if err != nil {
		return err
	}

	requests, err := polymorphichelpers.LogsForObjectFn(restClientGetter, pod, &corev1.PodLogOptions{Container: ctrName}, opts.GetPodTimeout, false)
	if err != nil {
		return err
	}
	for _, request := range requests {
		if err := logs.DefaultConsumeRequest(context.Background(), request, opts.Out); err != nil {
			return err
		}
	}

	return nil
}

func getRestartPolicy(cmd *cobra.Command, interactive bool) (corev1.RestartPolicy, error) {
	restart := cmdutil.GetFlagString(cmd, "restart")
	if len(restart) == 0 {
		if interactive {
			return corev1.RestartPolicyOnFailure, nil
		}
		return corev1.RestartPolicyAlways, nil
	}
	switch corev1.RestartPolicy(restart) {
	case corev1.RestartPolicyAlways:
		return corev1.RestartPolicyAlways, nil
	case corev1.RestartPolicyOnFailure:
		return corev1.RestartPolicyOnFailure, nil
	case corev1.RestartPolicyNever:
		return corev1.RestartPolicyNever, nil
	}
	return "", cmdutil.UsageErrorf(cmd, "invalid restart policy: %s", restart)
}

func verifyImagePullPolicy(cmd *cobra.Command) error {
	pullPolicy := cmdutil.GetFlagString(cmd, "image-pull-policy")
	switch corev1.PullPolicy(pullPolicy) {
	case corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever:
		return nil
	case "":
		return nil
	}
	return cmdutil.UsageErrorf(cmd, "invalid image pull policy: %s", pullPolicy)
}

func (o *RunOptions) generateService(f cmdutil.Factory, cmd *cobra.Command, paramsIn map[string]interface{}) (*RunObject, error) {
	generators := generateversioned.GeneratorFn("expose")
	generator, found := generators[generateversioned.ServiceV2GeneratorName]
	if !found {
		return nil, fmt.Errorf("missing service generator: %s", generateversioned.ServiceV2GeneratorName)
	}
	names := generator.ParamNames()

	params := map[string]interface{}{}
	for key, value := range paramsIn {
		_, isString := value.(string)
		if isString {
			params[key] = value
		}
	}

	name, found := params["name"]
	if !found || len(name.(string)) == 0 {
		return nil, fmt.Errorf("name is a required parameter")
	}
	selector, found := params["labels"]
	if !found || len(selector.(string)) == 0 {
		selector = fmt.Sprintf("run=%s", name.(string))
	}
	params["selector"] = selector

	if defaultName, found := params["default-name"]; !found || len(defaultName.(string)) == 0 {
		params["default-name"] = name
	}

	runObject, err := o.createGeneratedObject(f, cmd, generator, names, params, nil)
	if err != nil {
		return nil, err
	}

	if err := o.PrintObj(runObject.Object); err != nil {
		return nil, err
	}
	// separate yaml objects
	if o.PrintFlags.OutputFormat != nil && *o.PrintFlags.OutputFormat == "yaml" {
		fmt.Fprintln(o.Out, "---")
	}

	return runObject, nil
}

func (o *RunOptions) createGeneratedObject(f cmdutil.Factory, cmd *cobra.Command, generator generate.Generator, names []generate.GeneratorParam, params map[string]interface{}, overrider *cmdutil.Overrider) (*RunObject, error) {
	// 验证用户通过命令行传递的参数是否正确、完整，例如镜像名、端口号等
	err := generate.ValidateParams(names, params)
	if err != nil {
		return nil, err
	}

	// TODO: Validate flag usage against selected generator. More tricky since --expose was added.
	//根据用户输入的参数调用生成器（generate.Generator）生成资源对象。
	//例如：kubectl run mypod --image=nginx 会调用生成 Pod 的生成器。
	obj, err := generator.Generate(params)
	if err != nil {
		return nil, err
	}
	//确定资源类型，例如 Pod 的 GVK（Group、Version、Kind）。
	//查找 API Server 中该资源的 REST 映射（REST API 路径、操作方式）。
	mapper, err := f.ToRESTMapper()
	if err != nil {
		return nil, err
	}
	// run has compiled knowledge of the thing is creating
	gvks, _, err := scheme.Scheme.ObjectKinds(obj)
	if err != nil {
		return nil, err
	}
	mapping, err := mapper.RESTMapping(gvks[0].GroupKind(), gvks[0].Version)
	if err != nil {
		return nil, err
	}

	if overrider != nil {
		// 目的：支持对生成的对象进行修改，比如调整特定字段。
		//典型应用场景：根据用户指定的额外参数修改生成的 Pod 属性
		obj, err = overrider.Apply(obj)
		if err != nil {
			return nil, err
		}
	}
	//将该操作记录下来，用于审计和可追溯性。
	if err := o.Recorder.Record(obj); err != nil {
		klog.V(4).Infof("error recording current command: %v", err)
	}

	actualObj := obj
	if o.DryRunStrategy != cmdutil.DryRunClient {
		// 如果用户使用了 --dry-run 参数：
		//Client 模式：不会发送请求到 API Server，仅模拟资源创建。
		//Server 模式：将请求发送到 API Server，但不实际创建资源（只验证合法性）
		if err := util.CreateOrUpdateAnnotation(cmdutil.GetFlagBool(cmd, cmdutil.ApplyAnnotationsFlag), obj, scheme.DefaultJSONEncoder()); err != nil {
			return nil, err
		}
		client, err := f.ClientForMapping(mapping)
		if err != nil {
			return nil, err
		}
		actualObj, err = resource.
			NewHelper(client, mapping).
			DryRun(o.DryRunStrategy == cmdutil.DryRunServer).
			WithFieldManager(o.fieldManager).
			Create(o.Namespace, false, obj)
		if err != nil {
			return nil, err
		}
	} else {
		if meta, err := meta.Accessor(actualObj); err == nil && o.EnforceNamespace {
			meta.SetNamespace(o.Namespace)
		}
	}

	return &RunObject{
		Object:  actualObj,
		Mapping: mapping,
	}, nil
}

// ErrPodCompleted is returned by PodRunning or PodContainerRunning to indicate that
// the pod has already reached completed state.
var ErrPodCompleted = fmt.Errorf("pod ran to completion")

// podCompleted returns true if the pod has run to completion, false if the pod has not yet
// reached running state, or an error in any other case.
func podCompleted(event watch.Event) (bool, error) {
	switch event.Type {
	case watch.Deleted:
		return false, errors.NewNotFound(schema.GroupResource{Resource: "pods"}, "")
	}
	switch t := event.Object.(type) {
	case *corev1.Pod:
		switch t.Status.Phase {
		case corev1.PodFailed, corev1.PodSucceeded:
			return true, nil
		}
	}
	return false, nil
}

// podSucceeded returns true if the pod has run to completion, false if the pod has not yet
// reached running state, or an error in any other case.
func podSucceeded(event watch.Event) (bool, error) {
	switch event.Type {
	case watch.Deleted:
		return false, errors.NewNotFound(schema.GroupResource{Resource: "pods"}, "")
	}
	switch t := event.Object.(type) {
	case *corev1.Pod:
		return t.Status.Phase == corev1.PodSucceeded, nil
	}
	return false, nil
}

// podRunningAndReady returns true if the pod is running and ready, false if the pod has not
// yet reached those states, returns ErrPodCompleted if the pod has run to completion, or
// an error in any other case.
func podRunningAndReady(event watch.Event) (bool, error) {
	switch event.Type {
	case watch.Deleted:
		return false, errors.NewNotFound(schema.GroupResource{Resource: "pods"}, "")
	}
	switch t := event.Object.(type) {
	case *corev1.Pod:
		switch t.Status.Phase {
		case corev1.PodFailed, corev1.PodSucceeded:
			return false, ErrPodCompleted
		case corev1.PodRunning:
			conditions := t.Status.Conditions
			if conditions == nil {
				return false, nil
			}
			for i := range conditions {
				if conditions[i].Type == corev1.PodReady &&
					conditions[i].Status == corev1.ConditionTrue {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
