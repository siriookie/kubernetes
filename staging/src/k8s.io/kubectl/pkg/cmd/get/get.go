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

package get

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/spf13/cobra"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	kubernetesscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	watchtools "k8s.io/client-go/tools/watch"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/rawhttp"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/interrupt"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/utils/ptr"
)

// GetOptions contains the input to the get command.
type GetOptions struct {
	PrintFlags             *PrintFlags                                                                      //与打印选项相关的标志，可能包括输出格式（如 -o）、是否打印表头等。这些选项会控制如何格式化 kubectl get 的输出。
	ToPrinter              func(*meta.RESTMapping, *bool, bool, bool) (printers.ResourcePrinterFunc, error) //一个函数，用于将 RESTMapping 转换为实际的打印函数。打印函数的返回值会负责输出资源对象的具体内容。kubectl get 命令需要根据 ToPrinter 来决定如何展示数据。
	IsHumanReadablePrinter bool                                                                             // 标志是否使用“人类可读”的打印格式。这通常意味着输出会采用更易于阅读的格式，如表格形式。

	CmdParent string //当前命令的父命令名称。kubectl get 是 kubectl 命令的一部分，因此它的父命令是 kubectl 本身。

	resource.FilenameOptions //包含与文件名相关的选项，通常用于指定资源清单文件或 YAML/JSON 配置文件

	Raw       string //一个原始字符串，通常包含未经处理的输入或资源内容。可以用于直接传递给 Kubernetes API 或进行自定义处理。
	Watch     bool   //一个布尔值，指示是否启用 "watch" 模式，即持续监视资源的变化。如果设置为 true，kubectl get 将持续显示资源状态变化。
	WatchOnly bool   //如果设置为 true，kubectl get 将仅在 watch 模式下显示变化，不会显示初始的资源状态。
	ChunkSize int64  //用于分页的每块数据大小。通常在数据量较大的时候，分块获取数据可以避免一次性请求太多资源。

	OutputWatchEvents bool //是否在 watch 模式下输出事件。当 Kubernetes 资源发生变化时，kubectl get 会输出这些变化事件。

	LabelSelector     string //标签选择器，用于根据资源的标签过滤结果。格式通常为 key=value。
	FieldSelector     string //字段选择器，用于基于资源的字段值进行过滤。与 LabelSelector 类似，但更精确。
	AllNamespaces     bool   //如果设置为 true，kubectl get 将显示所有命名空间中的资源，而不仅仅是当前命名空间。
	Namespace         string //指定命名空间。与 AllNamespaces 配合使用，决定命令在哪个命名空间中运行。
	ExplicitNamespace bool   // 如果为 true，表示明确指定了命名空间。这个标志用于区分命名空间是由用户显式指定的，还是由默认的命名空间值推断出来的。
	Subresource       string //子资源的名称。例如，在获取一个 Pod 的日志时，log 是一个子资源。这个字段指定要获取的子资源名称。
	SortBy            string //资源排序的依据。可以按某个字段进行排序，通常用于按照时间、名称等排序。

	ServerPrint bool //如果设置为 true，表示输出来自服务器端的打印，而不是本地格式化的数据。这通常用于某些服务器端打印功能。

	NoHeaders      bool //如果设置为 true，在输出中不显示列标题。这个选项通常用于 kubectl get 的表格输出格式，决定是否包括表头。
	IgnoreNotFound bool //如果设置为 true，即使找不到指定的资源，kubectl get 也不会返回错误，而是跳过。

	genericiooptions.IOStreams
}

var (
	getLong = templates.LongDesc(i18n.T(`
		Display one or many resources.

		Prints a table of the most important information about the specified resources.
		You can filter the list using a label selector and the --selector flag. If the
		desired resource type is namespaced you will only see results in the current
		namespace if you don't specify any namespace.

		By specifying the output as 'template' and providing a Go template as the value
		of the --template flag, you can filter the attributes of the fetched resources.`))

	getExample = templates.Examples(i18n.T(`
		# List all pods in ps output format
		kubectl get pods

		# List all pods in ps output format with more information (such as node name)
		kubectl get pods -o wide

		# List a single replication controller with specified NAME in ps output format
		kubectl get replicationcontroller web

		# List deployments in JSON output format, in the "v1" version of the "apps" API group
		kubectl get deployments.v1.apps -o json

		# List a single pod in JSON output format
		kubectl get -o json pod web-pod-13je7

		# List a pod identified by type and name specified in "pod.yaml" in JSON output format
		kubectl get -f pod.yaml -o json

		# List resources from a directory with kustomization.yaml - e.g. dir/kustomization.yaml
		kubectl get -k dir/

		# Return only the phase value of the specified pod
		kubectl get -o template pod/web-pod-13je7 --template={{.status.phase}}

		# List resource information in custom columns
		kubectl get pod test-pod -o custom-columns=CONTAINER:.spec.containers[0].name,IMAGE:.spec.containers[0].image

		# List all replication controllers and services together in ps output format
		kubectl get rc,services

		# List one or more resources by their type and names
		kubectl get rc/web service/frontend pods/web-pod-13je7

		# List the 'status' subresource for a single pod
		kubectl get pod web-pod-13je7 --subresource status

		# List all deployments in namespace 'backend'
		kubectl get deployments.apps --namespace backend

		# List all pods existing in all namespaces
		kubectl get pods --all-namespaces`))
)

const (
	useServerPrintColumns = "server-print"
)

// NewGetOptions returns a GetOptions with default chunk size 500.
func NewGetOptions(parent string, streams genericiooptions.IOStreams) *GetOptions {
	return &GetOptions{
		PrintFlags: NewGetPrintFlags(),
		CmdParent:  parent,

		IOStreams:   streams,
		ChunkSize:   cmdutil.DefaultChunkSize,
		ServerPrint: true,
	}
}

// NewCmdGet creates a command object for the generic "get" action, which
// retrieves one or more resources from a server.
func NewCmdGet(parent string, f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := NewGetOptions(parent, streams)

	cmd := &cobra.Command{
		Use:                   fmt.Sprintf("get [(-o|--output=)%s] (TYPE[.VERSION][.GROUP] [NAME | -l label] | TYPE[.VERSION][.GROUP]/NAME ...) [flags]", strings.Join(o.PrintFlags.AllowedFormats(), "|")),
		DisableFlagsInUseLine: true, //设为 true，表示不在命令行中显示标志（例如 -o 等），仅显示命令本身。
		Short:                 i18n.T("Display one or many resources"),
		Long:                  getLong + "\n\n" + cmdutil.SuggestAPIResources(parent),
		Example:               getExample,
		// ValidArgsFunction is set when this function is called so that we have access to the util package
		// 当命令执行时，实际调用的函数。这里会执行 Complete（完成命令参数的填充）、Validate（验证参数是否合法）和 Run（执行实际操作）三个函数。
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.Run(f, args)) /* get指令实现的逻辑都在这里面了*/
		},
		SuggestFor: []string{"list", "ps"},
	}

	o.PrintFlags.AddFlags(cmd)

	cmd.Flags().StringVar(&o.Raw, "raw", o.Raw, "Raw URI to request from the server.  Uses the transport specified by the kubeconfig file.")
	cmd.Flags().BoolVarP(&o.Watch, "watch", "w", o.Watch, "After listing/getting the requested object, watch for changes.")
	cmd.Flags().BoolVar(&o.WatchOnly, "watch-only", o.WatchOnly, "Watch for changes to the requested object(s), without listing/getting first.")
	cmd.Flags().BoolVar(&o.OutputWatchEvents, "output-watch-events", o.OutputWatchEvents, "Output watch event objects when --watch or --watch-only is used. Existing objects are output as initial ADDED events.")
	cmd.Flags().BoolVar(&o.IgnoreNotFound, "ignore-not-found", o.IgnoreNotFound, "If the requested object does not exist the command will return exit code 0.")
	cmd.Flags().StringVar(&o.FieldSelector, "field-selector", o.FieldSelector, "Selector (field query) to filter on, supports '=', '==', and '!='.(e.g. --field-selector key1=value1,key2=value2). The server only supports a limited number of field queries per type.")
	cmd.Flags().BoolVarP(&o.AllNamespaces, "all-namespaces", "A", o.AllNamespaces, "If present, list the requested object(s) across all namespaces. Namespace in current context is ignored even if specified with --namespace.")
	addServerPrintColumnFlags(cmd, o)
	cmdutil.AddFilenameOptionFlags(cmd, &o.FilenameOptions, "identifying the resource to get from a server.")
	cmdutil.AddChunkSizeFlag(cmd, &o.ChunkSize)
	cmdutil.AddLabelSelectorFlagVar(cmd, &o.LabelSelector)
	cmdutil.AddSubresourceFlags(cmd, &o.Subresource, "If specified, gets the subresource of the requested object.")
	return cmd
}

// Complete takes the command arguments and factory and infers any remaining options.
func (o *GetOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	//如果 --raw 参数被设置，表示用户希望直接请求原始 URI，此时不允许传递任何其他参数。如果传递了参数，返回错误。
	if len(o.Raw) > 0 {
		if len(args) > 0 {
			return fmt.Errorf("arguments may not be passed when --raw is specified")
		}
		return nil
	}

	var err error
	//通过工厂方法 f.ToRawKubeConfigLoader().Namespace() 获取当前 Kubernetes 上下文中的命名空间，
	//并将其存储在 o.Namespace 中。如果设置了 --all-namespaces，则将 ExplicitNamespace 设置为 false，意味着忽略命名空间。
	o.Namespace, o.ExplicitNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}
	if o.AllNamespaces {
		o.ExplicitNamespace = false
	}

	if o.PrintFlags.HumanReadableFlags.SortBy != nil {
		o.SortBy = *o.PrintFlags.HumanReadableFlags.SortBy
	}

	o.NoHeaders = cmdutil.GetFlagBool(cmd, "no-headers")

	// TODO (soltysh): currently we don't support custom columns
	// with server side print. So in these cases force the old behavior.
	outputOption := cmd.Flags().Lookup("output").Value.String()
	//如果用户选择了 --output=custom-columns、--output=yaml 或 --output=json，则禁用服务器端打印（因为这些输出格式通常不支持服务器端打印）。
	//处理模板参数：
	if strings.Contains(outputOption, "custom-columns") || outputOption == "yaml" || strings.Contains(outputOption, "json") {
		o.ServerPrint = false
	}
	//获取 --template 参数的值，用于在输出时使用自定义模板格式。
	templateArg := ""
	if o.PrintFlags.TemplateFlags != nil && o.PrintFlags.TemplateFlags.TemplateArgument != nil {
		templateArg = *o.PrintFlags.TemplateFlags.TemplateArgument
	}
	//如果没有指定输出格式，或者输出格式为 wide，则认为是人类可读格式。
	// human readable printers have special conversion rules, so we determine if we're using one.
	if (len(*o.PrintFlags.OutputFormat) == 0 && len(templateArg) == 0) || *o.PrintFlags.OutputFormat == "wide" {
		o.IsHumanReadablePrinter = true
	}

	o.ToPrinter = func(mapping *meta.RESTMapping, outputObjects *bool, withNamespace bool, withKind bool) (printers.ResourcePrinterFunc, error) {
		// make a new copy of current flags / opts before mutating
		printFlags := o.PrintFlags.Copy()

		if mapping != nil {
			printFlags.SetKind(mapping.GroupVersionKind.GroupKind())
		}
		if withNamespace {
			printFlags.EnsureWithNamespace()
		}
		if withKind {
			printFlags.EnsureWithKind()
		}

		printer, err := printFlags.ToPrinter()
		if err != nil {
			return nil, err
		}
		printer, err = printers.NewTypeSetter(scheme.Scheme).WrapToPrinter(printer, nil)
		if err != nil {
			return nil, err
		}

		if len(o.SortBy) > 0 {
			printer = &SortingPrinter{Delegate: printer, SortField: o.SortBy}
		}
		if outputObjects != nil {
			printer = &skipPrinter{delegate: printer, output: outputObjects}
		}
		if o.ServerPrint {
			printer = &TablePrinter{Delegate: printer}
		}
		return printer.PrintObj, nil
	}

	switch {
	case o.Watch:
		if len(o.SortBy) > 0 {
			fmt.Fprintf(o.IOStreams.ErrOut, "warning: --watch requested, --sort-by will be ignored for watch events received\n")
		}
	case o.WatchOnly:
		if len(o.SortBy) > 0 {
			fmt.Fprintf(o.IOStreams.ErrOut, "warning: --watch-only requested, --sort-by will be ignored\n")
		}
	default:
		if len(args) == 0 && cmdutil.IsFilenameSliceEmpty(o.Filenames, o.Kustomize) {
			fmt.Fprintf(o.ErrOut, "You must specify the type of resource to get. %s\n\n", cmdutil.SuggestAPIResources(o.CmdParent))
			fullCmdName := cmd.Parent().CommandPath()
			usageString := "Required resource not specified."
			if len(fullCmdName) > 0 && cmdutil.IsSiblingCommandExists(cmd, "explain") {
				usageString = fmt.Sprintf("%s\nUse \"%s explain <resource>\" for a detailed description of that resource (e.g. %[2]s explain pods).", usageString, fullCmdName)
			}

			return cmdutil.UsageErrorf(cmd, "%s", usageString)
		}
	}

	return nil
}

// Validate checks the set of flags provided by the user.
func (o *GetOptions) Validate() error {
	//--raw 选项的验证：
	//如果 --raw 被设置，并且与其他会影响服务器请求或输出的选项一起使用（例如 --watch、--watch-only、--label-selector），则返回错误。--raw 是用来直接指定一个 URL 请求的，不允许与其他选项一起使用。
	//--raw 不能与 --output 一起使用，如果 --output 设置了格式，返回错误。
	//确保 --raw 提供的是一个有效的 URL 路径，如果无效，返回错误。
	if len(o.Raw) > 0 {
		if o.Watch || o.WatchOnly || len(o.LabelSelector) > 0 {
			return fmt.Errorf("--raw may not be specified with other flags that filter the server request or alter the output")
		}
		if o.PrintFlags.OutputFormat != nil && len(*o.PrintFlags.OutputFormat) > 0 {
			return fmt.Errorf("--raw and --output are mutually exclusive")
		}
		if _, err := url.ParseRequestURI(o.Raw); err != nil {
			return fmt.Errorf("--raw must be a valid URL path: %v", err)
		}
	}
	// 如果 --show-labels 被启用，并且 --output 选项的格式不为 wide，则返回错误。--show-labels 只能与 wide 格式兼容，不能与其他输出格式一起使用。
	if o.PrintFlags.HumanReadableFlags.ShowLabels != nil && *o.PrintFlags.HumanReadableFlags.ShowLabels && o.PrintFlags.OutputFormat != nil {
		outputOption := *o.PrintFlags.OutputFormat
		if outputOption != "" && outputOption != "wide" {
			return fmt.Errorf("--show-labels option cannot be used with %s printer", outputOption)
		}
	}
	//如果设置了 --output-watch-events，但没有同时设置 --watch 或 --watch-only，则返回错误。--output-watch-events 选项只能与 --watch 或 --watch-only 选项一起使用。
	if o.OutputWatchEvents && !(o.Watch || o.WatchOnly) {
		return fmt.Errorf("--output-watch-events option can only be used with --watch or --watch-only")
	}
	return nil
}

// OriginalPositioner and NopPositioner is required for swap/sort operations of data in table format
type OriginalPositioner interface {
	OriginalPosition(int) int
}

// NopPositioner and OriginalPositioner is required for swap/sort operations of data in table format
type NopPositioner struct{}

// OriginalPosition returns the original position from NopPositioner object
func (t *NopPositioner) OriginalPosition(ix int) int {
	return ix
}

// RuntimeSorter holds the required objects to perform sorting of runtime objects
type RuntimeSorter struct {
	field      string
	decoder    runtime.Decoder
	objects    []runtime.Object
	positioner OriginalPositioner
}

// Sort performs the sorting of runtime objects
func (r *RuntimeSorter) Sort() error {
	// a list is only considered "sorted" if there are 0 or 1 items in it
	// AND (if 1 item) the item is not a Table object
	if len(r.objects) == 0 {
		return nil
	}
	if len(r.objects) == 1 {
		_, isTable := r.objects[0].(*metav1.Table)
		if !isTable {
			return nil
		}
	}

	includesTable := false
	includesRuntimeObjs := false

	for _, obj := range r.objects {
		switch t := obj.(type) {
		case *metav1.Table:
			includesTable = true

			if sorter, err := NewTableSorter(t, r.field); err != nil {
				return err
			} else if err := sorter.Sort(); err != nil {
				return err
			}
		default:
			includesRuntimeObjs = true
		}
	}

	// we use a NopPositioner when dealing with Table objects
	// because the objects themselves are not swapped, but rather
	// the rows in each object are swapped / sorted.
	r.positioner = &NopPositioner{}

	if includesRuntimeObjs && includesTable {
		return fmt.Errorf("sorting is not supported on mixed Table and non-Table object lists")
	}
	if includesTable {
		return nil
	}

	// if not dealing with a Table response from the server, assume
	// all objects are runtime.Object as usual, and sort using old method.
	var err error
	if r.positioner, err = SortObjects(r.decoder, r.objects, r.field); err != nil {
		return err
	}
	return nil
}

// OriginalPosition returns the original position of a runtime object
func (r *RuntimeSorter) OriginalPosition(ix int) int {
	if r.positioner == nil {
		return 0
	}
	return r.positioner.OriginalPosition(ix)
}

// WithDecoder allows custom decoder to be set for testing
func (r *RuntimeSorter) WithDecoder(decoder runtime.Decoder) *RuntimeSorter {
	r.decoder = decoder
	return r
}

// NewRuntimeSorter returns a new instance of RuntimeSorter
func NewRuntimeSorter(objects []runtime.Object, sortBy string) *RuntimeSorter {
	parsedField, err := RelaxedJSONPathExpression(sortBy)
	if err != nil {
		parsedField = sortBy
	}

	return &RuntimeSorter{
		field:   parsedField,
		decoder: kubernetesscheme.Codecs.UniversalDecoder(),
		objects: objects,
	}
}

func (o *GetOptions) transformRequests(req *rest.Request) {
	if !o.ServerPrint || !o.IsHumanReadablePrinter {
		return
	}

	req.SetHeader("Accept", strings.Join([]string{
		fmt.Sprintf("application/json;as=Table;v=%s;g=%s", metav1.SchemeGroupVersion.Version, metav1.GroupName),
		fmt.Sprintf("application/json;as=Table;v=%s;g=%s", metav1beta1.SchemeGroupVersion.Version, metav1beta1.GroupName),
		"application/json",
	}, ","))

	// if sorting, ensure we receive the full object in order to introspect its fields via jsonpath
	if len(o.SortBy) > 0 {
		req.Param("includeObject", "Object")
	}
}

// Run performs the get operation.
// TODO: remove the need to pass these arguments, like other commands.
func (o *GetOptions) Run(f cmdutil.Factory, args []string) error {
	// 如果是指定了url啥啥的，就直接调用rawhttp.RawGet
	if len(o.Raw) > 0 {
		restClient, err := f.RESTClient()
		if err != nil {
			return err
		}
		return rawhttp.RawGet(restClient, o.IOStreams, o.Raw)
	}
	// 先list，再根据版本号去watch 然后输出到控制台
	if o.Watch || o.WatchOnly {
		return o.watch(f, args)
	}
	chunkSize := o.ChunkSize
	if len(o.SortBy) > 0 {
		// TODO(juanvallejo): in the future, we could have the client use chunking
		// to gather all results, then sort them all at the end to reduce server load.
		chunkSize = 0
	}

	r := f.NewBuilder().
		Unstructured().
		NamespaceParam(o.Namespace).DefaultNamespace().AllNamespaces(o.AllNamespaces).
		FilenameParam(o.ExplicitNamespace, &o.FilenameOptions).
		LabelSelectorParam(o.LabelSelector).
		FieldSelectorParam(o.FieldSelector).
		Subresource(o.Subresource).
		RequestChunksOf(chunkSize).
		ResourceTypeOrNameArgs(true, args...).
		ContinueOnError().
		Latest().
		Flatten().
		TransformRequests(o.transformRequests).
		Do()

	if o.IgnoreNotFound {
		r.IgnoreErrors(apierrors.IsNotFound)
	}
	if err := r.Err(); err != nil {
		return err
	}

	if !o.IsHumanReadablePrinter {
		return o.printGeneric(r)
	}

	allErrs := []error{}
	errs := sets.NewString()
	infos, err := r.Infos()
	if err != nil {
		allErrs = append(allErrs, err)
	}
	printWithKind := multipleGVKsRequested(infos)

	objs := make([]runtime.Object, len(infos))
	for ix := range infos {
		objs[ix] = infos[ix].Object
	}

	var positioner OriginalPositioner
	if len(o.SortBy) > 0 {
		sorter := NewRuntimeSorter(objs, o.SortBy)
		if err := sorter.Sort(); err != nil {
			return err
		}
		positioner = sorter
	}

	var printer printers.ResourcePrinter
	var lastMapping *meta.RESTMapping

	// track if we write any output
	trackingWriter := &trackingWriterWrapper{Delegate: o.Out}
	// output an empty line separating output
	separatorWriter := &separatorWriterWrapper{Delegate: trackingWriter}
	//这一行初始化了一个新的表格写入器 w，它会被用来按表格形式打印资源的内容。separatorWriter 是一个提供给写入器的参数，可能用于管理输出的分隔符。
	w := printers.GetNewTabWriter(separatorWriter)
	// 判断是否需要考虑命名空间。如果不打印所有命名空间（o.AllNamespaces 为 false），则默认资源是命名空间相关的
	allResourcesNamespaced := !o.AllNamespaces
	for ix := range objs {
		var mapping *meta.RESTMapping
		var info *resource.Info
		if positioner != nil {
			info = infos[positioner.OriginalPosition(ix)]
			mapping = info.Mapping
		} else {
			info = infos[ix]
			mapping = info.Mapping
		}

		allResourcesNamespaced = allResourcesNamespaced && info.Namespaced()
		printWithNamespace := o.AllNamespaces

		if mapping != nil && mapping.Scope.Name() == meta.RESTScopeNameRoot {
			printWithNamespace = false
		}
		//检查是否需要创建一个新的打印器（printer），如果当前的资源映射与上一个资源映射不同，则需要刷新表格并设置列宽。
		if shouldGetNewPrinterForMapping(printer, lastMapping, mapping) {
			w.Flush()
			w.SetRememberedWidths(nil)

			// add linebreaks between resource groups (if there is more than one)
			// when it satisfies all following 3 conditions:
			// 1) it's not the first resource group
			// 2) it has row header
			// 3) we've written output since the last time we started a new set of headers
			//如果不是第一个资源组，并且已经输出过内容，则添加分隔行。
			if lastMapping != nil && !o.NoHeaders && trackingWriter.Written > 0 {
				separatorWriter.SetReady(true)
			}
			//使用 o.ToPrinter 方法生成一个新的打印器，并根据资源映射和是否打印命名空间等条件配置它。如果发生错误，则记录错误并继续处理下一个资源。
			printer, err = o.ToPrinter(mapping, nil, printWithNamespace, printWithKind)
			if err != nil {
				if !errs.Has(err.Error()) {
					errs.Insert(err.Error())
					allErrs = append(allErrs, err)
				}
				continue
			}

			lastMapping = mapping
		}

		printer.PrintObj(info.Object, w)
	}
	//确保所有的内容都已被写入输出
	w.Flush()
	if trackingWriter.Written == 0 && !o.IgnoreNotFound && len(allErrs) == 0 {
		// if we wrote no output, and had no errors, and are not ignoring NotFound, be sure we output something
		if allResourcesNamespaced {
			fmt.Fprintf(o.ErrOut, "No resources found in %s namespace.\n", o.Namespace)
		} else {
			fmt.Fprintln(o.ErrOut, "No resources found")
		}
	}
	return utilerrors.NewAggregate(allErrs)
}

type trackingWriterWrapper struct {
	Delegate io.Writer
	Written  int
}

func (t *trackingWriterWrapper) Write(p []byte) (n int, err error) {
	t.Written += len(p)
	return t.Delegate.Write(p)
}

type separatorWriterWrapper struct {
	Delegate io.Writer
	Ready    bool
}

func (s *separatorWriterWrapper) Write(p []byte) (n int, err error) {
	// If we're about to write non-empty bytes and `s` is ready,
	// we prepend an empty line to `p` and reset `s.Read`.
	if len(p) != 0 && s.Ready {
		fmt.Fprintln(s.Delegate)
		s.Ready = false
	}
	return s.Delegate.Write(p)
}

func (s *separatorWriterWrapper) SetReady(state bool) {
	s.Ready = state
}

// watch starts a client-side watch of one or more resources.
// TODO: remove the need for arguments here.
func (o *GetOptions) watch(f cmdutil.Factory, args []string) error {
	r := f.NewBuilder().
		Unstructured().
		NamespaceParam(o.Namespace).DefaultNamespace().AllNamespaces(o.AllNamespaces).
		FilenameParam(o.ExplicitNamespace, &o.FilenameOptions). /* 要watch的file*/
		LabelSelectorParam(o.LabelSelector).
		FieldSelectorParam(o.FieldSelector).
		RequestChunksOf(o.ChunkSize).
		ResourceTypeOrNameArgs(true, args...).
		SingleResourceType().
		Latest().
		TransformRequests(o.transformRequests).
		Do()
	if err := r.Err(); err != nil {
		return err
	}
	infos, err := r.Infos()
	if err != nil {
		return err
	}
	if multipleGVKsRequested(infos) {
		return i18n.Errorf("watch is only supported on individual resources and resource collections - more than 1 resource was found")
	}

	info := infos[0]
	mapping := info.ResourceMapping()
	outputObjects := ptr.To(!o.WatchOnly)
	printer, err := o.ToPrinter(mapping, outputObjects, o.AllNamespaces, false)
	if err != nil {
		return err
	}
	obj, err := r.Object()
	if err != nil {
		return err
	}

	// watching from resourceVersion 0, starts the watch at ~now and
	// will return an initial watch event.  Starting form ~now, rather
	// the rv of the object will insure that we start the watch from
	// inside the watch window, which the rv of the object might not be.
	// 获取资源的版本号（resourceVersion）。对于列表类型的资源，
	//资源版本号通常是当前时刻（~now），并且没有初始的事件。
	//如果是列表类型的资源，会尝试获取列表的资源版本号。
	rv := "0"
	isList := meta.IsListType(obj)
	if isList {
		// the resourceVersion of list objects is ~now but won't return
		// an initial watch event
		rv, err = meta.NewAccessor().ResourceVersion(obj)
		if err != nil {
			return err
		}
	}

	writer := printers.GetNewTabWriter(o.Out)

	// print the current object
	var objsToPrint []runtime.Object
	if isList {
		objsToPrint, _ = meta.ExtractList(obj)
	} else {
		objsToPrint = append(objsToPrint, obj)
	}
	for _, objToPrint := range objsToPrint {
		if o.OutputWatchEvents {
			objToPrint = &metav1.WatchEvent{Type: string(watch.Added), Object: runtime.RawExtension{Object: objToPrint}}
		}
		if err := printer.PrintObj(objToPrint, writer); err != nil {
			return fmt.Errorf("unable to output the provided object: %v", err)
		}
	}
	writer.Flush()
	if isList {
		// we can start outputting objects now, watches started from lists don't emit synthetic added events
		*outputObjects = true
	} else {
		// suppress output, since watches started for individual items emit a synthetic ADDED event first
		*outputObjects = false
	}

	// print watched changes
	//启动资源的 watch，传入资源版本号。这样就能从指定的版本开始监听后续的事件。
	w, err := r.Watch(rv)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	intr := interrupt.New(nil, cancel)
	//将流中的事件输出到控制台（或其他 writer 目标）中。
	intr.Run(func() error {
		_, err := watchtools.UntilWithoutRetry(ctx, w, func(e watch.Event) (bool, error) {
			objToPrint := e.Object
			if o.OutputWatchEvents {
				objToPrint = &metav1.WatchEvent{Type: string(e.Type), Object: runtime.RawExtension{Object: objToPrint}}
			}
			if err := printer.PrintObj(objToPrint, writer); err != nil {
				return false, err
			}
			writer.Flush()
			// after processing at least one event, start outputting objects
			*outputObjects = true
			return false, nil
		})
		return err
	})
	return nil
}

func (o *GetOptions) printGeneric(r *resource.Result) error {
	// we flattened the data from the builder, so we have individual items, but now we'd like to either:
	// 1. if there is more than one item, combine them all into a single list
	// 2. if there is a single item and that item is a list, leave it as its specific list
	// 3. if there is a single item and it is not a list, leave it as a single item
	var errs []error
	singleItemImplied := false
	infos, err := r.IntoSingleItemImplied(&singleItemImplied).Infos()
	if err != nil {
		if singleItemImplied {
			return err
		}
		errs = append(errs, err)
	}

	if len(infos) == 0 && o.IgnoreNotFound {
		return utilerrors.Reduce(utilerrors.Flatten(utilerrors.NewAggregate(errs)))
	}

	printer, err := o.ToPrinter(nil, nil, false, false)
	if err != nil {
		return err
	}

	var obj runtime.Object
	if !singleItemImplied || len(infos) != 1 {
		// we have zero or multple items, so coerce all items into a list.
		// we don't want an *unstructured.Unstructured list yet, as we
		// may be dealing with non-unstructured objects. Compose all items
		// into an corev1.List, and then decode using an unstructured scheme.
		list := corev1.List{
			TypeMeta: metav1.TypeMeta{
				Kind:       "List",
				APIVersion: "v1",
			},
			ListMeta: metav1.ListMeta{},
		}
		for _, info := range infos {
			list.Items = append(list.Items, runtime.RawExtension{Object: info.Object})
		}

		listData, err := json.Marshal(list)
		if err != nil {
			return err
		}

		converted, err := runtime.Decode(unstructured.UnstructuredJSONScheme, listData)
		if err != nil {
			return err
		}

		obj = converted
	} else {
		obj = infos[0].Object
	}

	isList := meta.IsListType(obj)
	if isList {
		items, err := meta.ExtractList(obj)
		if err != nil {
			return err
		}

		// take the items and create a new list for display
		list := &unstructured.UnstructuredList{
			Object: map[string]interface{}{
				"kind":       "List",
				"apiVersion": "v1",
				"metadata":   map[string]interface{}{},
			},
		}
		if listMeta, err := meta.ListAccessor(obj); err == nil {
			list.Object["metadata"] = map[string]interface{}{
				"resourceVersion": listMeta.GetResourceVersion(),
			}
		}

		for _, item := range items {
			list.Items = append(list.Items, *item.(*unstructured.Unstructured))
		}
		if err := printer.PrintObj(list, o.Out); err != nil {
			errs = append(errs, err)
		}
		return utilerrors.Reduce(utilerrors.Flatten(utilerrors.NewAggregate(errs)))
	}

	if printErr := printer.PrintObj(obj, o.Out); printErr != nil {
		errs = append(errs, printErr)
	}

	return utilerrors.Reduce(utilerrors.Flatten(utilerrors.NewAggregate(errs)))
}

func addServerPrintColumnFlags(cmd *cobra.Command, opt *GetOptions) {
	cmd.Flags().BoolVar(&opt.ServerPrint, useServerPrintColumns, opt.ServerPrint, "If true, have the server return the appropriate table output. Supports extension APIs and CRDs.")
}

func shouldGetNewPrinterForMapping(printer printers.ResourcePrinter, lastMapping, mapping *meta.RESTMapping) bool {
	return printer == nil || lastMapping == nil || mapping == nil || mapping.Resource != lastMapping.Resource
}

// multipleGVKsRequested 用来判断资源列表中是否包含多个不同的资源类型。如果列表中的资源类型不一致（即 GVK 不同），
// 它会返回 true；如果所有资源的类型和版本相同，返回 false。
func multipleGVKsRequested(infos []*resource.Info) bool {
	if len(infos) < 2 {
		return false
	}
	gvk := infos[0].Mapping.GroupVersionKind
	for _, info := range infos {
		if info.Mapping.GroupVersionKind != gvk {
			return true
		}
	}
	return false
}
