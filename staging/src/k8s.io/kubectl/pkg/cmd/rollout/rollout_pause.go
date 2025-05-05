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

package rollout

import (
	"fmt"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/kubectl/pkg/cmd/set"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// PauseOptions is the start of the data required to perform the operation.  As new fields are added, add them here instead of
// referencing the cmd.Flags()
type PauseOptions struct {
	PrintFlags *genericclioptions.PrintFlags
	ToPrinter  func(string) (printers.ResourcePrinter, error)

	Pauser           polymorphichelpers.ObjectPauserFunc
	Builder          func() *resource.Builder
	Namespace        string
	EnforceNamespace bool
	Resources        []string
	LabelSelector    string

	resource.FilenameOptions
	genericiooptions.IOStreams

	fieldManager string
}

var (
	pauseLong = templates.LongDesc(i18n.T(`
		Mark the provided resource as paused.

		Paused resources will not be reconciled by a controller.
		Use "kubectl rollout resume" to resume a paused resource.
		Currently only deployments support being paused.`))

	pauseExample = templates.Examples(`
		# Mark the nginx deployment as paused
		# Any current state of the deployment will continue its function; new updates
		# to the deployment will not have an effect as long as the deployment is paused
		kubectl rollout pause deployment/nginx`)
)

// NewCmdRolloutPause returns a Command instance for 'rollout pause' sub command
// 示例
// （1）暂停 Deployment 更新
// kubectl rollout pause deployment my-deployment
// 效果：
//
// my-deployment 的 .spec 变更不会立即触发滚动更新
//
// 需要 kubectl rollout resume 解除暂停
func NewCmdRolloutPause(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	o := &PauseOptions{
		PrintFlags: genericclioptions.NewPrintFlags("paused").WithTypeSetter(scheme.Scheme),
		IOStreams:  streams,
	}

	validArgs := []string{"deployment"}

	cmd := &cobra.Command{
		Use:                   "pause RESOURCE",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Mark the provided resource as paused"),
		Long:                  pauseLong,
		Example:               pauseExample,
		ValidArgsFunction:     completion.SpecifiedResourceTypeAndNameCompletionFunc(f, validArgs),
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(f, cmd, args))
			cmdutil.CheckErr(o.Validate())
			cmdutil.CheckErr(o.RunPause())
		},
	}

	o.PrintFlags.AddFlags(cmd)

	usage := "identifying the resource to get from a server."
	cmdutil.AddFilenameOptionFlags(cmd, &o.FilenameOptions, usage)
	cmdutil.AddFieldManagerFlagVar(cmd, &o.fieldManager, "kubectl-rollout")
	cmdutil.AddLabelSelectorFlagVar(cmd, &o.LabelSelector)
	return cmd
}

// Complete completes all the required options
func (o *PauseOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, args []string) error {
	o.Pauser = polymorphichelpers.ObjectPauserFn

	var err error
	o.Namespace, o.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	o.Resources = args
	o.Builder = f.NewBuilder

	o.ToPrinter = func(operation string) (printers.ResourcePrinter, error) {
		o.PrintFlags.NamePrintFlags.Operation = operation
		return o.PrintFlags.ToPrinter()
	}

	return nil
}

func (o *PauseOptions) Validate() error {
	if len(o.Resources) == 0 && cmdutil.IsFilenameSliceEmpty(o.Filenames, o.Kustomize) {
		return fmt.Errorf("required resource not specified")
	}
	return nil
}

// RunPause performs the execution of 'rollout pause' sub command
func (o *PauseOptions) RunPause() error {
	r := o.Builder().
		WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
		NamespaceParam(o.Namespace).DefaultNamespace().
		LabelSelectorParam(o.LabelSelector).
		FilenameParam(o.EnforceNamespace, &o.FilenameOptions).
		ResourceTypeOrNameArgs(true, o.Resources...).
		ContinueOnError().
		Latest().
		Flatten().
		Do()
	if err := r.Err(); err != nil {
		return err
	}

	allErrs := []error{}
	infos, err := r.Infos()
	if err != nil {
		// restore previous command behavior where
		// an error caused by retrieving infos due to
		// at least a single broken object did not result
		// in an immediate return, but rather an overall
		// aggregation of errors.
		allErrs = append(allErrs, err)
	}
	// 传入需要变更的函数，计算出patches
	patches := set.CalculatePatches(infos, scheme.DefaultJSONEncoder(), set.PatchFn(o.Pauser))

	if len(patches) == 0 && len(allErrs) == 0 {
		fmt.Fprintf(o.ErrOut, "No resources found in %s namespace.\n", o.Namespace)
		return nil
	}

	for _, patch := range patches {
		info := patch.Info
		//如果 patch.Err 发生错误，则记录错误信息，跳过该资源，继续处理其他资源。
		if patch.Err != nil {
			resourceString := info.Mapping.Resource.Resource
			if len(info.Mapping.Resource.Group) > 0 {
				resourceString = resourceString + "." + info.Mapping.Resource.Group
			}
			allErrs = append(allErrs, fmt.Errorf("error: %s %q %v", resourceString, info.Name, patch.Err))
			continue
		}
		//如果 patch.Patch 为空，说明资源已经处于暂停状态，则直接打印 "already paused"，跳过该资源。
		if string(patch.Patch) == "{}" || len(patch.Patch) == 0 {
			printer, err := o.ToPrinter("already paused")
			if err != nil {
				allErrs = append(allErrs, err)
				continue
			}
			if err = printer.PrintObj(info.Object, o.Out); err != nil {
				allErrs = append(allErrs, err)
			}
			continue
		}
		//调用 Patch 方法，向 Kubernetes API 服务器发送 Patch 请求，修改资源，将 spec.paused = true。
		obj, err := resource.NewHelper(info.Client, info.Mapping).
			WithFieldManager(o.fieldManager).
			Patch(info.Namespace, info.Name, types.StrategicMergePatchType, patch.Patch, nil)
		if err != nil {
			allErrs = append(allErrs, fmt.Errorf("failed to patch: %v", err))
			continue
		}

		info.Refresh(obj, true)
		//使用 o.ToPrinter("paused") 生成输出，并打印 "paused" 状态。
		printer, err := o.ToPrinter("paused")
		if err != nil {
			allErrs = append(allErrs, err)
			continue
		}
		if err = printer.PrintObj(info.Object, o.Out); err != nil {
			allErrs = append(allErrs, err)
		}
	}
	//如果有多个错误，NewAggregate 将它们组合成一个错误返回，而不是立即终止，保证所有资源都被尝试处理。
	return utilerrors.NewAggregate(allErrs)
}
