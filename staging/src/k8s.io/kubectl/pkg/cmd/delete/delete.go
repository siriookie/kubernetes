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

package delete

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	cmdwait "k8s.io/kubectl/pkg/cmd/wait"
	"k8s.io/kubectl/pkg/rawhttp"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/kubectl/pkg/util/term"
)

var (
	deleteLong = templates.LongDesc(i18n.T(`
		Delete resources by file names, stdin, resources and names, or by resources and label selector.

		JSON and YAML formats are accepted. Only one type of argument may be specified: file names,
		resources and names, or resources and label selector.

		Some resources, such as pods, support graceful deletion. These resources define a default period
		before they are forcibly terminated (the grace period) but you may override that value with
		the --grace-period flag, or pass --now to set a grace-period of 1. Because these resources often
		represent entities in the cluster, deletion may not be acknowledged immediately. If the node
		hosting a pod is down or cannot reach the API server, termination may take significantly longer
		than the grace period. To force delete a resource, you must specify the --force flag.
		Note: only a subset of resources support graceful deletion. In absence of the support,
		the --grace-period flag is ignored.

		IMPORTANT: Force deleting pods does not wait for confirmation that the pod's processes have been
		terminated, which can leave those processes running until the node detects the deletion and
		completes graceful deletion. If your processes use shared storage or talk to a remote API and
		depend on the name of the pod to identify themselves, force deleting those pods may result in
		multiple processes running on different machines using the same identification which may lead
		to data corruption or inconsistency. Only force delete pods when you are sure the pod is
		terminated, or if your application can tolerate multiple copies of the same pod running at once.
		Also, if you force delete pods, the scheduler may place new pods on those nodes before the node
		has released those resources and causing those pods to be evicted immediately.

		Note that the delete command does NOT do resource version checks, so if someone submits an
		update to a resource right when you submit a delete, their update will be lost along with the
		rest of the resource.

		After a CustomResourceDefinition is deleted, invalidation of discovery cache may take up
		to 6 hours. If you don't want to wait, you might want to run "kubectl api-resources" to refresh
		the discovery cache.`))

	deleteExample = templates.Examples(i18n.T(`
		# Delete a pod using the type and name specified in pod.json
		kubectl delete -f ./pod.json

		# Delete resources from a directory containing kustomization.yaml - e.g. dir/kustomization.yaml
		kubectl delete -k dir

		# Delete resources from all files that end with '.json'
		kubectl delete -f '*.json'

		# Delete a pod based on the type and name in the JSON passed into stdin
		cat pod.json | kubectl delete -f -

		# Delete pods and services with same names "baz" and "foo"
		kubectl delete pod,service baz foo

		# Delete pods and services with label name=myLabel
		kubectl delete pods,services -l name=myLabel

		# Delete a pod with minimal delay
		kubectl delete pod foo --now

		# Force delete a pod on a dead node
		kubectl delete pod foo --force

		# Delete all pods
		kubectl delete pods --all

		# Delete all pods only if the user confirms the deletion
		kubectl delete pods --all --interactive`))
)

type DeleteOptions struct {
	resource.FilenameOptions

	LabelSelector       string
	FieldSelector       string
	DeleteAll           bool
	DeleteAllNamespaces bool
	CascadingStrategy   metav1.DeletionPropagation
	IgnoreNotFound      bool
	DeleteNow           bool
	ForceDeletion       bool
	WaitForDeletion     bool
	Quiet               bool
	WarnClusterScope    bool
	Raw                 string
	Interactive         bool

	GracePeriod int
	Timeout     time.Duration

	DryRunStrategy cmdutil.DryRunStrategy

	Output string

	DynamicClient      dynamic.Interface
	Mapper             meta.RESTMapper
	Result             *resource.Result
	PreviewResult      *resource.Result
	previewResourceMap map[cmdwait.ResourceLocation]struct{}

	genericiooptions.IOStreams
	WarningPrinter *printers.WarningPrinter
}

// 示例
// 删除 pod
// kubectl delete pod nginx
// 删除所有 pod
// kubectl delete pod --all
// 使用 label 选择器删除
// kubectl delete pod -l app=myapp
// 删除 YAML 文件定义的资源
// kubectl delete -f my-deployment.yaml
func NewCmdDelete(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	deleteFlags := NewDeleteCommandFlags("containing the resource to delete.")
	//-f：从文件中删除资源
	//
	//-k：从 Kustomization 目录中删除资源
	//
	//--all：删除所有匹配的资源
	//-l：使用 label 选择器删除资源
	cmd := &cobra.Command{
		Use:                   "delete ([-f FILENAME] | [-k DIRECTORY] | TYPE [(NAME | -l label | --all)])",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Delete resources by file names, stdin, resources and names, or by resources and label selector"),
		Long:                  deleteLong,
		Example:               deleteExample,
		ValidArgsFunction:     completion.ResourceTypeAndNameCompletionFunc(f),
		Run: func(cmd *cobra.Command, args []string) {
			o, err := deleteFlags.ToOptions(nil, streams)
			cmdutil.CheckErr(err)
			cmdutil.CheckErr(o.Complete(f, args, cmd))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.RunDelete(f))
		},
		SuggestFor: []string{"rm"},
	}

	deleteFlags.AddFlags(cmd)
	cmdutil.AddDryRunFlag(cmd)

	return cmd
}

func (o *DeleteOptions) Complete(f cmdutil.Factory, args []string, cmd *cobra.Command) error {
	cmdNamespace, enforceNamespace, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	o.WarnClusterScope = enforceNamespace && !o.DeleteAllNamespaces

	if o.DeleteAll || len(o.LabelSelector) > 0 || len(o.FieldSelector) > 0 {
		if f := cmd.Flags().Lookup("ignore-not-found"); f != nil && !f.Changed {
			// If the user didn't explicitly set the option, default to ignoring NotFound errors when used with --all, -l, or --field-selector
			o.IgnoreNotFound = true
		}
	}
	if o.DeleteNow {
		if o.GracePeriod != -1 {
			return fmt.Errorf("--now and --grace-period cannot be specified together")
		}
		o.GracePeriod = 1
	}
	if o.GracePeriod == 0 && !o.ForceDeletion {
		// To preserve backwards compatibility, but prevent accidental data loss, we convert --grace-period=0
		// into --grace-period=1. Users may provide --force to bypass this conversion.
		o.GracePeriod = 1
	}
	if o.ForceDeletion && o.GracePeriod < 0 {
		o.GracePeriod = 0
	}

	o.DryRunStrategy, err = cmdutil.GetDryRunStrategy(cmd)
	if err != nil {
		return err
	}

	// Set default WarningPrinter if not already set.
	if o.WarningPrinter == nil {
		o.WarningPrinter = printers.NewWarningPrinter(o.ErrOut, printers.WarningPrinterOptions{Color: term.AllowsColorOutput(o.ErrOut)})
	}

	if len(o.Raw) != 0 {
		return nil
	}

	r := f.NewBuilder().
		Unstructured().
		ContinueOnError().
		NamespaceParam(cmdNamespace).DefaultNamespace().
		FilenameParam(enforceNamespace, &o.FilenameOptions).
		LabelSelectorParam(o.LabelSelector).
		FieldSelectorParam(o.FieldSelector).
		SelectAllParam(o.DeleteAll).
		AllNamespaces(o.DeleteAllNamespaces).
		ResourceTypeOrNameArgs(false, args...).RequireObject(false).
		Flatten().
		Do()
	err = r.Err()
	if err != nil {
		return err
	}
	o.Result = r

	if o.Interactive {
		// preview result will be used to list resources for confirmation prior to actual delete.
		// We can not use r as result object because it can only be used once. But we need to traverse
		// twice. Parameters in preview result must be equal to genuine result.
		previewr := f.NewBuilder().
			Unstructured().
			ContinueOnError().
			NamespaceParam(cmdNamespace).DefaultNamespace().
			FilenameParam(enforceNamespace, &o.FilenameOptions).
			LabelSelectorParam(o.LabelSelector).
			FieldSelectorParam(o.FieldSelector).
			SelectAllParam(o.DeleteAll).
			AllNamespaces(o.DeleteAllNamespaces).
			ResourceTypeOrNameArgs(false, args...).RequireObject(false).
			Flatten().
			Do()
		err = previewr.Err()
		if err != nil {
			return err
		}
		o.PreviewResult = previewr
		o.previewResourceMap = make(map[cmdwait.ResourceLocation]struct{})
	}

	o.Mapper, err = f.ToRESTMapper()
	if err != nil {
		return err
	}

	o.DynamicClient, err = f.DynamicClient()
	if err != nil {
		return err
	}

	return nil
}

func (o *DeleteOptions) Validate() error {
	if o.Output != "" && o.Output != "name" {
		return fmt.Errorf("unexpected -o output mode: %v. We only support '-o name'", o.Output)
	}

	if o.DeleteAll && len(o.LabelSelector) > 0 {
		return fmt.Errorf("cannot set --all and --selector at the same time")
	}
	if o.DeleteAll && len(o.FieldSelector) > 0 {
		return fmt.Errorf("cannot set --all and --field-selector at the same time")
	}
	if o.WarningPrinter == nil {
		return fmt.Errorf("WarningPrinter can not be used without initialization")
	}

	switch {
	case o.GracePeriod == 0 && o.ForceDeletion:
		o.WarningPrinter.Print("Immediate deletion does not wait for confirmation that the running resource has been terminated. The resource may continue to run on the cluster indefinitely.")
	case o.GracePeriod > 0 && o.ForceDeletion:
		return fmt.Errorf("--force and --grace-period greater than 0 cannot be specified together")
	}

	if len(o.Raw) == 0 {
		return nil
	}

	if o.Interactive {
		return fmt.Errorf("--interactive can not be used with --raw")
	}
	if len(o.FilenameOptions.Filenames) > 1 {
		return fmt.Errorf("--raw can only use a single local file or stdin")
	} else if len(o.FilenameOptions.Filenames) == 1 {
		if strings.Index(o.FilenameOptions.Filenames[0], "http://") == 0 || strings.Index(o.FilenameOptions.Filenames[0], "https://") == 0 {
			return fmt.Errorf("--raw cannot read from a url")
		}
	}

	if o.FilenameOptions.Recursive {
		return fmt.Errorf("--raw and --recursive are mutually exclusive")
	}
	if len(o.Output) > 0 {
		return fmt.Errorf("--raw and --output are mutually exclusive")
	}
	if _, err := url.ParseRequestURI(o.Raw); err != nil {
		return fmt.Errorf("--raw must be a valid URL path: %v", err)
	}

	return nil
}

func (o *DeleteOptions) RunDelete(f cmdutil.Factory) error {
	if len(o.Raw) > 0 {
		//o.Raw：如果 o.Raw 选项不为空，则执行 原始 HTTP 请求删除，适用于 kubectl delete --raw
		restClient, err := f.RESTClient()
		if err != nil {
			return err
		}
		if len(o.Filenames) == 0 {
			return rawhttp.RawDelete(restClient, o.IOStreams, o.Raw, "")
		}
		return rawhttp.RawDelete(restClient, o.IOStreams, o.Raw, o.Filenames[0])
	}
	//o.Interactive：如果启用了交互式模式（--interactive），则：
	//
	//创建 previewInfos：存储要删除的资源信息。
	//
	//忽略 NotFound 错误（如果 o.IgnoreNotFound 为 true）。
	if o.Interactive {
		previewInfos := []*resource.Info{}
		if o.IgnoreNotFound {
			o.PreviewResult = o.PreviewResult.IgnoreErrors(errors.IsNotFound)
		}
		//遍历 o.PreviewResult（即将删除的资源列表）：
		//
		//遇到错误直接返回。
		//
		//将资源信息添加到 previewInfos 列表中。
		//
		//记录资源信息到 o.previewResourceMap，用于后续确认。
		err := o.PreviewResult.Visit(func(info *resource.Info, err error) error {
			if err != nil {
				return err
			}
			previewInfos = append(previewInfos, info)
			o.previewResourceMap[cmdwait.ResourceLocation{
				GroupResource: info.Mapping.Resource.GroupResource(),
				Namespace:     info.Namespace,
				Name:          info.Name,
			}] = struct{}{}

			return nil
		})
		if err != nil {
			return err
		}
		//如果没有资源可删除，直接返回 "No resources found"。
		if len(previewInfos) == 0 {
			fmt.Fprintf(o.Out, "No resources found\n")
			return nil
		}

		//调用 o.confirmation(previewInfos) 获取用户确认：
		//
		//用户确认：继续删除。
		//
		//用户取消：打印 "deletion is cancelled"，然后退出。
		if !o.confirmation(previewInfos) {
			fmt.Fprintf(o.Out, "deletion is cancelled\n")
			return nil
		}
	}

	return o.DeleteResult(o.Result)
}

func (o *DeleteOptions) DeleteResult(r *resource.Result) error {
	//found：记录成功找到的资源数量。
	//
	//o.IgnoreNotFound：如果启用了 忽略不存在资源，那么 r.IgnoreErrors(errors.IsNotFound) 过滤掉 NotFound 错误。
	//
	//warnClusterScope：如果删除的是 集群级别资源，则打印警告。
	//
	//deletedInfos：存储删除的资源信息。
	//
	//uidMap：用于记录 资源的 UID，以便后续 等待删除完成。
	found := 0
	if o.IgnoreNotFound {
		r = r.IgnoreErrors(errors.IsNotFound)
	}
	warnClusterScope := o.WarnClusterScope
	deletedInfos := []*resource.Info{}
	uidMap := cmdwait.UIDMap{}
	err := r.Visit(func(info *resource.Info, err error) error {
		if err != nil {
			return err
		}
		// 交互模式下，只删除用户确认的资源
		if o.Interactive {
			if _, ok := o.previewResourceMap[cmdwait.ResourceLocation{
				GroupResource: info.Mapping.Resource.GroupResource(),
				Namespace:     info.Namespace,
				Name:          info.Name,
			}]; !ok {
				// resource not in the list of previewed resources based on resourceLocation
				return nil
			}
		}

		deletedInfos = append(deletedInfos, info)
		found++

		options := &metav1.DeleteOptions{}
		//如果 o.GracePeriod >= 0，设置 GracePeriodSeconds（优雅退出时间）。
		//
		//设置 PropagationPolicy：
		//
		//Foreground：先删除子资源，再删除父资源（默认）。
		//
		//Background：直接删除父资源，由 API 负责级联删除子资源。
		if o.GracePeriod >= 0 {
			options = metav1.NewDeleteOptions(int64(o.GracePeriod))
		}
		options.PropagationPolicy = &o.CascadingStrategy
		//如果 删除的是集群范围的资源（如 ClusterRole、Namespace），则打印警告。
		if warnClusterScope && info.Mapping.Scope.Name() == meta.RESTScopeNameRoot {
			o.WarningPrinter.Print("deleting cluster-scoped resources, not scoped to the provided namespace")
			warnClusterScope = false
		}
		//如果是 Dry-Run 模式（仅模拟执行删除，不真正删除），打印资源信息后直接返回。
		if o.DryRunStrategy == cmdutil.DryRunClient {
			if !o.Quiet {
				o.PrintObj(info)
			}
			return nil
		}
		//o.deleteResource(info, options) 通过 API 调用 真正删除资源。
		response, err := o.deleteResource(info, options)
		if err != nil {
			return err
		}
		//获取删除后资源的 UID：
		//
		//如果 response 是 metav1.Status，则从 status.Details.UID 读取 UID。
		//
		//如果 response 是其他对象，则使用 meta.Accessor(response).GetUID() 获取 UID。
		//
		//这些 UID 存入 uidMap，以便 等待删除完成。
		resourceLocation := cmdwait.ResourceLocation{
			GroupResource: info.Mapping.Resource.GroupResource(),
			Namespace:     info.Namespace,
			Name:          info.Name,
		}
		if status, ok := response.(*metav1.Status); ok && status.Details != nil {
			uidMap[resourceLocation] = status.Details.UID
			return nil
		}
		responseMetadata, err := meta.Accessor(response)
		if err != nil {
			// we don't have UID, but we didn't fail the delete, next best thing is just skipping the UID
			klog.V(1).Info(err)
			return nil
		}
		uidMap[resourceLocation] = responseMetadata.GetUID()

		return nil
	})
	if err != nil {
		return err
	}
	//如果没有找到要删除的资源，输出提示并返回。
	if found == 0 {
		fmt.Fprintf(o.Out, "No resources found\n")
		return nil
	}
	//如果未启用等待 (--wait=false)，直接返回。
	if !o.WaitForDeletion {
		return nil
	}
	// if we don't have a dynamic client, we don't want to wait.  Eventually when delete is cleaned up, this will likely
	// drop out.
	//如果 DynamicClient 为空，表示不支持等待删除，直接返回。
	if o.DynamicClient == nil {
		return nil
	}

	// If we are dry-running, then we don't want to wait
	if o.DryRunStrategy != cmdutil.DryRunNone {
		return nil
	}
	//o.Timeout == 0 表示无限等待，设定默认超时 168 小时（1 周）。
	effectiveTimeout := o.Timeout
	if effectiveTimeout == 0 {
		// if we requested to wait forever, set it to a week.
		effectiveTimeout = 168 * time.Hour
	}
	//cmdwait.WaitOptions 负责 轮询检查资源是否被删除：
	//
	//ResourceFinder：资源查找器，用于遍历 deletedInfos。
	//
	//UIDMap：记录资源 UID。
	//
	//DynamicClient：用于检查资源是否仍然存在。
	//
	//Timeout：等待超时时间。
	//
	//ConditionFn：等待资源被删除的条件。
	waitOptions := cmdwait.WaitOptions{
		ResourceFinder: genericclioptions.ResourceFinderForResult(resource.InfoListVisitor(deletedInfos)),
		UIDMap:         uidMap,
		DynamicClient:  o.DynamicClient,
		Timeout:        effectiveTimeout,

		Printer:     printers.NewDiscardingPrinter(),
		ConditionFn: cmdwait.IsDeleted,
		IOStreams:   o.IOStreams,
	}
	// 等待资源删除成功
	err = waitOptions.RunWait()
	if errors.IsForbidden(err) || errors.IsMethodNotSupported(err) {
		// if we're forbidden from waiting, we shouldn't fail.
		// if the resource doesn't support a verb we need, we shouldn't fail.
		klog.V(1).Info(err)
		return nil
	}
	return err
}

func (o *DeleteOptions) deleteResource(info *resource.Info, deleteOptions *metav1.DeleteOptions) (runtime.Object, error) {
	deleteResponse, err := resource.
		NewHelper(info.Client, info.Mapping).
		DryRun(o.DryRunStrategy == cmdutil.DryRunServer).
		DeleteWithOptions(info.Namespace, info.Name, deleteOptions)
	if err != nil {
		return nil, cmdutil.AddSourceToErr("deleting", info.Source, err)
	}

	if !o.Quiet {
		o.PrintObj(info)
	}
	return deleteResponse, nil
}

// PrintObj for deleted objects is special because we do not have an object to print.
// This mirrors name printer behavior
func (o *DeleteOptions) PrintObj(info *resource.Info) {
	operation := "deleted"
	groupKind := info.Mapping.GroupVersionKind
	kindString := fmt.Sprintf("%s.%s", strings.ToLower(groupKind.Kind), groupKind.Group)
	if len(groupKind.Group) == 0 {
		kindString = strings.ToLower(groupKind.Kind)
	}

	if o.GracePeriod == 0 {
		operation = "force deleted"
	}

	switch o.DryRunStrategy {
	case cmdutil.DryRunClient:
		operation = fmt.Sprintf("%s (dry run)", operation)
	case cmdutil.DryRunServer:
		operation = fmt.Sprintf("%s (server dry run)", operation)
	}

	if o.Output == "name" {
		// -o name: prints resource/name
		fmt.Fprintf(o.Out, "%s/%s\n", kindString, info.Name)
		return
	}

	// understandable output by default
	fmt.Fprintf(o.Out, "%s \"%s\" %s\n", kindString, info.Name, operation)
}

func (o *DeleteOptions) confirmation(infos []*resource.Info) bool {
	fmt.Fprintf(o.Out, i18n.T("You are about to delete the following %d resource(s):\n"), len(infos))
	for _, info := range infos {
		groupKind := info.Mapping.GroupVersionKind
		kindString := fmt.Sprintf("%s.%s", strings.ToLower(groupKind.Kind), groupKind.Group)
		if len(groupKind.Group) == 0 {
			kindString = strings.ToLower(groupKind.Kind)
		}

		fmt.Fprintf(o.Out, "%s/%s\n", kindString, info.Name)
	}
	fmt.Fprint(o.Out, i18n.T("Do you want to continue?")+" (y/n): ")
	var input string
	_, err := fmt.Fscan(o.In, &input)
	if err != nil {
		return false
	}

	return strings.EqualFold(input, "y")
}
