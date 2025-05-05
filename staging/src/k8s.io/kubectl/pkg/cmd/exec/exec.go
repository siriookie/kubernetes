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

package exec

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	dockerterm "github.com/moby/term"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"k8s.io/apimachinery/pkg/api/meta"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/cmd/util/podcmd"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/interrupt"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/kubectl/pkg/util/term"
)

var (
	execExample = templates.Examples(i18n.T(`
		# Get output from running the 'date' command from pod mypod, using the first container by default
		kubectl exec mypod -- date

		# Get output from running the 'date' command in ruby-container from pod mypod
		kubectl exec mypod -c ruby-container -- date

		# Switch to raw terminal mode; sends stdin to 'bash' in ruby-container from pod mypod
		# and sends stdout/stderr from 'bash' back to the client
		kubectl exec mypod -c ruby-container -i -t -- bash -il

		# List contents of /usr from the first container of pod mypod and sort by modification time
		# If the command you want to execute in the pod has any flags in common (e.g. -i),
		# you must use two dashes (--) to separate your command's flags/arguments
		# Also note, do not surround your command and its flags/arguments with quotes
		# unless that is how you would execute it normally (i.e., do ls -t /usr, not "ls -t /usr")
		kubectl exec mypod -i -t -- ls -t /usr

		# Get output from running 'date' command from the first pod of the deployment mydeployment, using the first container by default
		kubectl exec deploy/mydeployment -- date

		# Get output from running 'date' command from the first pod of the service myservice, using the first container by default
		kubectl exec svc/myservice -- date
		`))
)

const (
	defaultPodExecTimeout = 60 * time.Second
)

func NewCmdExec(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	options := &ExecOptions{
		StreamOptions: StreamOptions{
			IOStreams: streams,
		},

		Executor: &DefaultRemoteExecutor{},
	}
	cmd := &cobra.Command{
		Use:                   "exec (POD | TYPE/NAME) [-c CONTAINER] [flags] -- COMMAND [args...]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Execute a command in a container"),
		Long:                  i18n.T("Execute a command in a container."),
		Example:               execExample,
		ValidArgsFunction:     completion.PodResourceNameCompletionFunc(f),
		Run: func(cmd *cobra.Command, args []string) {
			argsLenAtDash := cmd.ArgsLenAtDash()
			cmdutil.CheckErr(options.Complete(f, cmd, args, argsLenAtDash))
			cmdutil.CheckErr(options.Validate())
			cmdutil.CheckErr(options.Run())
		},
	}
	cmdutil.AddPodRunningTimeoutFlag(cmd, defaultPodExecTimeout)
	cmdutil.AddJsonFilenameFlag(cmd.Flags(), &options.FilenameOptions.Filenames, "to use to exec into the resource")
	// TODO support UID
	cmdutil.AddContainerVarFlags(cmd, &options.ContainerName, options.ContainerName)
	cmdutil.CheckErr(cmd.RegisterFlagCompletionFunc("container", completion.ContainerCompletionFunc(f)))

	cmd.Flags().BoolVarP(&options.Stdin, "stdin", "i", options.Stdin, "Pass stdin to the container")
	cmd.Flags().BoolVarP(&options.TTY, "tty", "t", options.TTY, "Stdin is a TTY")
	cmd.Flags().BoolVarP(&options.Quiet, "quiet", "q", options.Quiet, "Only print output from the remote session")
	return cmd
}

// RemoteExecutor defines the interface accepted by the Exec command - provided for test stubbing
type RemoteExecutor interface {
	Execute(url *url.URL, config *restclient.Config, stdin io.Reader, stdout, stderr io.Writer, tty bool, terminalSizeQueue remotecommand.TerminalSizeQueue) error
}

// DefaultRemoteExecutor is the standard implementation of remote command execution
type DefaultRemoteExecutor struct{}

func (*DefaultRemoteExecutor) Execute(url *url.URL, config *restclient.Config, stdin io.Reader, stdout, stderr io.Writer, tty bool, terminalSizeQueue remotecommand.TerminalSizeQueue) error {
	exec, err := createExecutor(url, config)
	if err != nil {
		return err
	}
	return exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdin:             stdin,
		Stdout:            stdout,
		Stderr:            stderr,
		Tty:               tty,
		TerminalSizeQueue: terminalSizeQueue,
	})
}

// createExecutor returns the Executor or an error if one occurred.
func createExecutor(url *url.URL, config *restclient.Config) (remotecommand.Executor, error) {
	exec, err := remotecommand.NewSPDYExecutor(config, "POST", url)
	if err != nil {
		return nil, err
	}
	// Fallback executor is default, unless feature flag is explicitly disabled.
	if !cmdutil.RemoteCommandWebsockets.IsDisabled() {
		// WebSocketExecutor must be "GET" method as described in RFC 6455 Sec. 4.1 (page 17).
		websocketExec, err := remotecommand.NewWebSocketExecutor(config, "GET", url.String())
		if err != nil {
			return nil, err
		}
		exec, err = remotecommand.NewFallbackExecutor(websocketExec, exec, func(err error) bool {
			return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
		})
		if err != nil {
			return nil, err
		}
	}
	return exec, nil
}

type StreamOptions struct {
	Namespace     string
	PodName       string
	ContainerName string
	Stdin         bool
	TTY           bool
	// minimize unnecessary output
	Quiet bool
	// InterruptParent, if set, is used to handle interrupts while attached
	InterruptParent *interrupt.Handler

	genericiooptions.IOStreams

	// for testing
	overrideStreams func() (io.ReadCloser, io.Writer, io.Writer)
	isTerminalIn    func(t term.TTY) bool
}

// ExecOptions declare the arguments accepted by the Exec command
type ExecOptions struct {
	StreamOptions
	resource.FilenameOptions

	ResourceName     string
	Command          []string
	EnforceNamespace bool

	Builder          func() *resource.Builder
	ExecutablePodFn  polymorphichelpers.AttachablePodForObjectFunc
	restClientGetter genericclioptions.RESTClientGetter

	Pod           *corev1.Pod
	Executor      RemoteExecutor
	PodClient     coreclient.PodsGetter
	GetPodTimeout time.Duration
	Config        *restclient.Config
}

// Complete verifies command line arguments and loads data from the command environment
func (p *ExecOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, argsIn []string, argsLenAtDash int) error {
	if len(argsIn) > 0 && argsLenAtDash != 0 {
		p.ResourceName = argsIn[0]
	}
	if argsLenAtDash > -1 {
		p.Command = argsIn[argsLenAtDash:]
	} else if len(argsIn) > 1 || (len(argsIn) > 0 && len(p.FilenameOptions.Filenames) != 0) {
		return cmdutil.UsageErrorf(cmd, "exec [POD] [COMMAND] is not supported anymore. Use exec [POD] -- [COMMAND] instead")
	}

	var err error
	p.Namespace, p.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	p.ExecutablePodFn = polymorphichelpers.AttachablePodForObjectFn

	p.GetPodTimeout, err = cmdutil.GetPodRunningTimeoutFlag(cmd)
	if err != nil {
		return cmdutil.UsageErrorf(cmd, "%s", err.Error())
	}

	p.Builder = f.NewBuilder
	p.restClientGetter = f

	p.Config, err = f.ToRESTConfig()
	if err != nil {
		return err
	}

	clientset, err := f.KubernetesClientSet()
	if err != nil {
		return err
	}
	p.PodClient = clientset.CoreV1()

	return nil
}

// Validate checks that the provided exec options are specified.
func (p *ExecOptions) Validate() error {
	if len(p.PodName) == 0 && len(p.ResourceName) == 0 && len(p.FilenameOptions.Filenames) == 0 {
		return fmt.Errorf("pod, type/name or --filename must be specified")
	}
	if len(p.Command) == 0 {
		return fmt.Errorf("you must specify at least one command for the container")
	}
	if p.Out == nil || p.ErrOut == nil {
		return fmt.Errorf("both output and error output must be provided")
	}
	return nil
}

func (o *StreamOptions) SetupTTY() term.TTY {
	t := term.TTY{
		Parent: o.InterruptParent,
		Out:    o.Out,
	}

	if !o.Stdin {
		// need to nil out o.In to make sure we don't create a stream for stdin
		o.In = nil
		o.TTY = false
		return t
	}

	t.In = o.In
	if !o.TTY {
		return t
	}

	if o.isTerminalIn == nil {
		o.isTerminalIn = func(tty term.TTY) bool {
			return tty.IsTerminalIn()
		}
	}
	if !o.isTerminalIn(t) {
		o.TTY = false

		if !o.Quiet && o.ErrOut != nil {
			fmt.Fprintln(o.ErrOut, "Unable to use a TTY - input is not a terminal or the right kind of file")
		}

		return t
	}

	// if we get to here, the user wants to attach stdin, wants a TTY, and o.In is a terminal, so we
	// can safely set t.Raw to true
	t.Raw = true

	if o.overrideStreams == nil {
		// use dockerterm.StdStreams() to get the right I/O handles on Windows
		o.overrideStreams = dockerterm.StdStreams
	}
	stdin, stdout, _ := o.overrideStreams()
	o.In = stdin
	t.In = stdin
	if o.Out != nil {
		o.Out = stdout
		t.Out = stdout
	}

	return t
}

// Run executes a validated remote execution against a pod.
// 实现了 kubectl exec 命令的远程执行操作，用于在 Kubernetes 集群中的某个 Pod 内部执行命令
// kubectl exec -it my-pod -- /bin/sh
// 命令说明
// kubectl exec：在 Pod 内执行命令。
// -it：
// -i（interactive）：保持标准输入（stdin）打开，用于交互模式。
// -t（tty）：分配一个伪终端，使终端交互更友好（类似 SSH）。
// my-pod：Pod 名称（替换成你的 Pod）。
// -- /bin/sh：
// --：用于区分 kubectl 命令参数和要在容器内执行的命令。
// /bin/sh：在容器内执行 sh Shell（如果是 Ubuntu 或 Debian，可以改为 /bin/bash）。
func (p *ExecOptions) Run() error {
	var err error
	// we still need legacy pod getter when PodName in ExecOptions struct is provided,
	// since there are any other command run this function by providing Podname with PodsGetter
	// and without resource builder, eg: `kubectl cp`.
	if len(p.PodName) != 0 {
		//第一种情况（kubectl exec -it POD_NAME -- COMMAND）
		//如果 p.PodName 存在，说明用户直接提供了 Pod 名称，则通过 p.PodClient.Pods(p.Namespace).Get() 查询该 Pod。
		p.Pod, err = p.PodClient.Pods(p.Namespace).Get(context.TODO(), p.PodName, metav1.GetOptions{})
		if err != nil {
			return err
		}
	} else {
		//第二种情况（kubectl exec -it -f POD_FILE -- COMMAND）
		//如果 p.PodName 为空，说明用户提供的是YAML/JSON 文件或Pod 资源名。
		//builder.Do().Object() 解析资源，找到目标 Pod。
		//如果 obj 是一个列表，则报错，因为 kubectl exec 只能针对单个 Pod 执行。
		builder := p.Builder().
			WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
			FilenameParam(p.EnforceNamespace, &p.FilenameOptions).
			NamespaceParam(p.Namespace).DefaultNamespace()
		if len(p.ResourceName) > 0 {
			builder = builder.ResourceNames("pods", p.ResourceName)
		}

		obj, err := builder.Do().Object()
		if err != nil {
			return err
		}

		if meta.IsListType(obj) {
			return fmt.Errorf("cannot exec into multiple objects at a time")
		}

		p.Pod, err = p.ExecutablePodFn(p.restClientGetter, obj, p.GetPodTimeout)
		if err != nil {
			return err
		}
	}

	pod := p.Pod
	//如果 Pod 处于 Succeeded（成功）或 Failed（失败）状态，则报错，无法执行命令。
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return fmt.Errorf("cannot exec into a container in a completed pod; current phase is %s", pod.Status.Phase)
	}

	containerName := p.ContainerName
	if len(containerName) == 0 {
		//p.ContainerName 为空，说明用户没有显式指定 --container，则从 Pod 中选择默认的容器。
		//FindOrDefaultContainerByName() 会找到第一个匹配的容器。
		container, err := podcmd.FindOrDefaultContainerByName(pod, containerName, p.Quiet, p.ErrOut)
		if err != nil {
			return err
		}
		containerName = container.Name
	}

	// ensure we can recover the terminal while attached
	//SetupTTY() 负责初始化 TTY 交互环境，确保 kubectl exec -it 在交互模式下正确运行。
	t := p.SetupTTY()

	var sizeQueue remotecommand.TerminalSizeQueue
	if t.Raw {
		// this call spawns a goroutine to monitor/update the terminal size
		//MonitorSize() 监测终端窗口大小变化，确保 kubectl exec 过程中窗口大小调整不会导致显示问题。
		sizeQueue = t.MonitorSize(t.GetSize())

		// unset p.Err if it was previously set because both stdout and stderr go over p.Out when tty is
		// true
		p.ErrOut = nil
	}

	fn := func() error {
		restClient, err := restclient.RESTClientFor(p.Config)
		if err != nil {
			return err
		}

		// TODO: consider abstracting into a client invocation or client helper
		req := restClient.Post().
			Resource("pods").
			Name(pod.Name).
			Namespace(pod.Namespace).
			SubResource("exec")
		req.VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   p.Command,
			Stdin:     p.Stdin,
			Stdout:    p.Out != nil,
			Stderr:    p.ErrOut != nil,
			TTY:       t.Raw,
		}, scheme.ParameterCodec)

		return p.Executor.Execute(req.URL(), p.Config, p.In, p.Out, p.ErrOut, t.Raw, sizeQueue)
	}

	if err := t.Safe(fn); err != nil {
		return err
	}

	return nil
}
