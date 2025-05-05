/*
Copyright 2017 The Kubernetes Authors.

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

package editor

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strings"

	"github.com/spf13/cobra"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/mergepatch"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/cli-runtime/pkg/resource"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/cmd/util/editor/crlf"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util"
)

// EditOptions contains all the options for running edit cli command.
type EditOptions struct {
	resource.FilenameOptions
	RecordFlags *genericclioptions.RecordFlags

	PrintFlags *genericclioptions.PrintFlags
	ToPrinter  func(string) (printers.ResourcePrinter, error)

	OutputPatch        bool
	WindowsLineEndings bool

	cmdutil.ValidateOptions
	ValidationDirective string

	OriginalResult *resource.Result

	EditMode EditMode

	CmdNamespace    string
	ApplyAnnotation bool
	ChangeCause     string

	managedFields map[types.UID][]metav1.ManagedFieldsEntry

	genericiooptions.IOStreams

	Recorder            genericclioptions.Recorder
	f                   cmdutil.Factory
	editPrinterOptions  *editPrinterOptions
	updatedResultGetter func(data []byte) *resource.Result

	FieldManager string

	Subresource string
}

// NewEditOptions returns an initialized EditOptions instance
func NewEditOptions(editMode EditMode, ioStreams genericiooptions.IOStreams) *EditOptions {
	return &EditOptions{
		RecordFlags: genericclioptions.NewRecordFlags(),

		EditMode: editMode,

		PrintFlags: genericclioptions.NewPrintFlags("edited").WithTypeSetter(scheme.Scheme),

		editPrinterOptions: &editPrinterOptions{
			// create new editor-specific PrintFlags, with all
			// output flags disabled, except json / yaml
			printFlags: (&genericclioptions.PrintFlags{
				JSONYamlPrintFlags: genericclioptions.NewJSONYamlPrintFlags(),
			}).WithDefaultOutput("yaml"),
			ext:       ".yaml",
			addHeader: true,
		},

		WindowsLineEndings: goruntime.GOOS == "windows",

		Recorder: genericclioptions.NoopRecorder{},

		IOStreams: ioStreams,
	}
}

type editPrinterOptions struct {
	printFlags *genericclioptions.PrintFlags
	ext        string
	addHeader  bool
}

func (e *editPrinterOptions) Complete(fromPrintFlags *genericclioptions.PrintFlags) error {
	if e.printFlags == nil {
		return fmt.Errorf("missing PrintFlags in editor printer options")
	}

	// bind output format from existing printflags
	if fromPrintFlags != nil && len(*fromPrintFlags.OutputFormat) > 0 {
		e.printFlags.OutputFormat = fromPrintFlags.OutputFormat
	}

	// prevent a commented header at the top of the user's
	// default editor if presenting contents as json.
	if *e.printFlags.OutputFormat == "json" {
		e.addHeader = false
		e.ext = ".json"
		return nil
	}

	// we default to yaml if check above is false, as only json or yaml are supported
	e.addHeader = true
	e.ext = ".yaml"
	return nil
}

func (e *editPrinterOptions) PrintObj(obj runtime.Object, out io.Writer) error {
	p, err := e.printFlags.ToPrinter()
	if err != nil {
		return err
	}

	return p.PrintObj(obj, out)
}

// Complete completes all the required options
func (o *EditOptions) Complete(f cmdutil.Factory, args []string, cmd *cobra.Command) error {
	var err error

	o.RecordFlags.Complete(cmd)
	o.Recorder, err = o.RecordFlags.ToRecorder()
	if err != nil {
		return err
	}

	if o.EditMode != NormalEditMode && o.EditMode != EditBeforeCreateMode && o.EditMode != ApplyEditMode {
		return fmt.Errorf("unsupported edit mode %q", o.EditMode)
	}

	o.editPrinterOptions.Complete(o.PrintFlags)

	if o.OutputPatch && o.EditMode != NormalEditMode {
		return fmt.Errorf("the edit mode doesn't support output the patch")
	}

	cmdNamespace, enforceNamespace, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}
	b := f.NewBuilder().
		Unstructured()
	if o.EditMode == NormalEditMode || o.EditMode == ApplyEditMode {
		// when do normal edit or apply edit we need to always retrieve the latest resource from server
		b = b.ResourceTypeOrNameArgs(true, args...).Latest()
	}
	r := b.NamespaceParam(cmdNamespace).DefaultNamespace().
		FilenameParam(enforceNamespace, &o.FilenameOptions).
		Subresource(o.Subresource).
		ContinueOnError().
		Flatten().
		Do()
	err = r.Err()
	if err != nil {
		return err
	}
	o.OriginalResult = r

	o.updatedResultGetter = func(data []byte) *resource.Result {
		// resource builder to read objects from edited data
		return f.NewBuilder().
			Unstructured().
			Stream(bytes.NewReader(data), "edited-file").
			Subresource(o.Subresource).
			ContinueOnError().
			Flatten().
			Do()
	}

	o.ToPrinter = func(operation string) (printers.ResourcePrinter, error) {
		o.PrintFlags.NamePrintFlags.Operation = operation
		return o.PrintFlags.ToPrinter()
	}

	o.ValidationDirective, err = cmdutil.GetValidationDirective(cmd)
	if err != nil {
		return err
	}

	o.CmdNamespace = cmdNamespace
	o.f = f

	return nil
}

// Validate checks the EditOptions to see if there is sufficient information to run the command.
func (o *EditOptions) Validate() error {
	return nil
}

// Run performs the execution
// kubectl edit svc my-service
// 打开编辑器 → 用户修改 YAML/JSON。
// 保存并退出 → kubectl 会检测变更并应用。
// API 服务器处理请求 → Kubernetes 调度、更新资源。
// 可能触发滚动更新（例如 Deployment）。
func (o *EditOptions) Run() error {
	edit := NewDefaultEditor(editorEnvs())
	//这句话的意思是 editFn 这个函数在每个编辑会话中都会被调用，具体来说，它有两种调用方式：
	//
	//普通编辑（normal edit）：
	//
	//editFn 只会调用一次，并且传入的是一个资源列表，即一次性处理多个资源的编辑操作。
	//创建时编辑（edit-on-create）：
	//
	//editFn 会对每个单独的资源分别调用一次，而不是一次性传入整个资源列表。也就是说，每个新创建的资源都会单独调用 editFn 进行处理。
	// editFn is invoked for each edit session (once with a list for normal edit, once for each individual resource in a edit-on-create invocation)
	editFn := func(infos []*resource.Info) error {
		var (
			results  = editResults{}
			original = []byte{}
			edited   = []byte{}
			file     string
			err      error
		)

		containsError := false
		// loop until we succeed or cancel editing
		for {
			// get the object we're going to serialize as input to the editor
			var originalObj runtime.Object
			switch len(infos) {
			case 1:
				originalObj = infos[0].Object
			default:
				l := &unstructured.UnstructuredList{
					Object: map[string]interface{}{
						"kind":       "List",
						"apiVersion": "v1",
						"metadata":   map[string]interface{}{},
					},
				}
				for _, info := range infos {
					l.Items = append(l.Items, *info.Object.(*unstructured.Unstructured))
				}
				originalObj = l
			}

			// generate the file to edit
			// 1. 创建一个 bytes.Buffer 作为写入目标
			buf := &bytes.Buffer{}
			var w io.Writer = buf
			// 2. 如果启用了 Windows 换行符 (CRLF)，则使用 CRLF Writer
			//o.WindowsLineEndings 是一个布尔值，如果为 true，表示需要使用 Windows 的 \r\n 作为换行符。
			//crlf.NewCRLFWriter(w) 会创建一个包装后的 Writer，自动转换 \n 为 \r\n，确保 Windows 兼容性。
			if o.WindowsLineEndings {
				w = crlf.NewCRLFWriter(w)
			}
			// 3. 如果需要添加头部信息，则写入头部
			//o.editPrinterOptions.addHeader 是一个布尔值，如果为 true，表示需要在文件顶部添加一个注释头（例如 YAML 资源的描述信息）。
			if o.editPrinterOptions.addHeader {
				results.header.writeTo(w, o.EditMode)
			}

			if !containsError {
				// 4.1 如果没有错误，处理对象并写入缓冲区
				if err := o.extractManagedFields(originalObj); err != nil {
					return preservedFile(err, results.file, o.ErrOut)
				}

				if err := o.editPrinterOptions.PrintObj(originalObj, w); err != nil {
					return preservedFile(err, results.file, o.ErrOut)
				}
				original = buf.Bytes()
			} else {
				// In case of an error, preserve the edited file.
				// Remove the comments (header) from it since we already
				// have included the latest header in the buffer above.
				// 4.2 如果有错误，保留已编辑的文件内容
				// 并移除已经添加的头部信息
				buf.Write(cmdutil.ManualStrip(edited))
			}

			// launch the editor
			// 启动编辑器
			editedDiff := edited
			//这部分代码创建一个临时文件并打开默认编辑器（通常是 vi 或 vim），让用户编辑 Kubernetes 资源的 YAML/JSON。
			//edit.LaunchTempFile(...)：
			//filepath.Base(os.Args[0]) 获取当前可执行文件的名称（如 kubectl）。
			//o.editPrinterOptions.ext 指定文件扩展名（可能是 .yaml 或 .json）。
			//buf 作为文件的初始内容（即当前资源的 YAML/JSON 配置）。
			//用户修改后保存退出，edited 变量存储用户编辑后的内容，file 记录临时文件路径。
			edited, file, err = edit.LaunchTempFile(fmt.Sprintf("%s-edit-", filepath.Base(os.Args[0])), o.editPrinterOptions.ext, buf)
			if err != nil {
				return preservedFile(err, results.file, o.ErrOut)
			}

			// If we're retrying the loop because of an error, and no change was made in the file, short-circuit
			//如果之前编辑出错（containsError == true），但这次编辑和上次内容相同，直接取消编辑，返回错误：
			//cmdutil.StripComments(editedDiff) 和 cmdutil.StripComments(edited) 会去掉 YAML/JSON 文件中的注释，然后进行比较。
			//如果没有修改，则报 "Edit cancelled, no valid changes were saved."，并退出。
			if containsError && bytes.Equal(cmdutil.StripComments(editedDiff), cmdutil.StripComments(edited)) {
				return preservedFile(fmt.Errorf("%s", "Edit cancelled, no valid changes were saved."), file, o.ErrOut)
			}
			// cleanup any file from the previous pass
			//results.file 记录上次的临时文件路径，如果它存在，说明上一次的编辑产生了一个文件，这里需要删除它，避免重复使用。
			if len(results.file) > 0 {
				os.Remove(results.file)
			}
			//打印用户修改后的内容
			klog.V(4).Infof("User edited:\n%s", string(edited))

			// Apply validation
			//o.f.Validator(o.ValidationDirective) 获取 Kubernetes API 服务器的 schema（模式），用于校验 YAML/JSON 结构是否合法。
			//schema.ValidateBytes(cmdutil.StripComments(edited)) 去掉注释后，检查 edited 是否符合 API 规范。
			//如果校验失败：
			//containsError = true 标记错误。
			//使用 apierrors.NewInvalid(...) 生成错误信息，提示**“The edited file failed validation”**。
			//continue 重新进入 for 循环，让用户重新编辑。
			schema, err := o.f.Validator(o.ValidationDirective)
			if err != nil {
				return preservedFile(err, file, o.ErrOut)
			}
			err = schema.ValidateBytes(cmdutil.StripComments(edited))
			if err != nil {
				results = editResults{
					file: file,
				}
				containsError = true
				fmt.Fprintln(o.ErrOut, results.addError(apierrors.NewInvalid(corev1.SchemeGroupVersion.WithKind("").GroupKind(),
					"", field.ErrorList{field.Invalid(nil, "The edited file failed validation", fmt.Sprintf("%v", err))}), infos[0]))
				continue
			}

			// Compare content without comments
			//如果编辑后的文件和原始文件相同（忽略注释），说明用户没有做任何修改。
			//直接删除临时文件 os.Remove(file)，并输出 "Edit cancelled, no changes made."。
			if bytes.Equal(cmdutil.StripComments(original), cmdutil.StripComments(edited)) {
				os.Remove(file)
				fmt.Fprintln(o.ErrOut, "Edit cancelled, no changes made.")
				return nil
			}
			//hasLines(bytes.NewBuffer(edited)) 检查文件是否包含有效内容：
			//如果 edited 为空，则删除文件 os.Remove(file)，并提示 "Edit cancelled, saved file was empty."。
			lines, err := hasLines(bytes.NewBuffer(edited))
			if err != nil {
				return preservedFile(err, file, o.ErrOut)
			}
			if !lines {
				os.Remove(file)
				fmt.Fprintln(o.ErrOut, "Edit cancelled, saved file was empty.")
				return nil
			}
			//editResults{file: file} 记录当前编辑的文件路径，以便后续错误处理或提交更新。
			results = editResults{
				file: file,
			}

			// parse the edited file
			//o.updatedResultGetter(edited).Infos() 解析用户编辑后的 Kubernetes 资源。
			//如果解析失败（YAML/JSON 语法错误）：
			//containsError = true 记录错误状态。
			//results.header.reasons 追加错误信息 “The edited file had a syntax error”。
			//continue 让用户重新编辑。
			updatedInfos, err := o.updatedResultGetter(edited).Infos()
			if err != nil {
				// syntax error
				containsError = true
				results.header.reasons = append(results.header.reasons, editReason{head: fmt.Sprintf("The edited file had a syntax error: %v", err)})
				continue
			}

			// not a syntax error as it turns out...
			//解析成功，说明 用户的 YAML/JSON 语法正确，将 containsError 设为 false。
			//updatedVisitor := resource.InfoListVisitor(updatedInfos)：
			//updatedInfos 是用户编辑后的 Kubernetes 资源。
			//InfoListVisitor(updatedInfos) 用于遍历这些资源（即 kubectl edit 支持一次修改多个资源）。
			containsError = false
			updatedVisitor := resource.InfoListVisitor(updatedInfos)

			// we need to add back managedFields to both updated and original object
			//managedFields 是 Kubernetes 用于管理 Server-Side Apply 变更记录的字段。
			//这部分逻辑确保：
			//用户编辑后的对象（updatedInfos）和原始对象（infos）都保留 managedFields。
			//如果恢复失败，则报错 preservedFile(err, file, o.ErrOut) 并退出。
			if err := o.restoreManagedFields(updatedInfos); err != nil {
				return preservedFile(err, file, o.ErrOut)
			}
			if err := o.restoreManagedFields(infos); err != nil {
				return preservedFile(err, file, o.ErrOut)
			}

			// need to make sure the original namespace wasn't changed while editing
			//resource.RequireNamespace(o.CmdNamespace) 用于检查 用户是否修改了 namespace。
			//Kubernetes 规定：
			//kubectl edit 不能修改 namespace（即 metadata.namespace 必须和原始对象一致）。
			//如果发现 namespace 被改动，报错并退出。
			if err := updatedVisitor.Visit(resource.RequireNamespace(o.CmdNamespace)); err != nil {
				return preservedFile(err, file, o.ErrOut)
			}

			// iterate through all items to apply annotations
			//o.visitAnnotation(updatedVisitor) 遍历所有编辑后的资源，应用必要的 annotations（注解）。
			//可能涉及：
			//记录 kubectl edit 的修改历史（类似 kubectl.kubernetes.io/last-applied-configuration）。
			//其他 K8s API 需要的元数据。
			if err := o.visitAnnotation(updatedVisitor); err != nil {
				return preservedFile(err, file, o.ErrOut)
			}

			switch o.EditMode {
			//NormalEditMode：常规的编辑模式，调用 visitToPatch 方法处理资源更新（例如通过 kubectl apply）。
			//ApplyEditMode：调用 visitToApplyEditPatch 方法处理编辑后的资源更新操作。
			//EditBeforeCreateMode：用于资源创建前的编辑，调用 visitToCreate 方法。
			//default：如果编辑模式不受支持，返回错误。
			case NormalEditMode:
				err = o.visitToPatch(infos, updatedVisitor, &results)
			case ApplyEditMode:
				err = o.visitToApplyEditPatch(infos, updatedVisitor)
			case EditBeforeCreateMode:
				err = o.visitToCreate(updatedVisitor)
			default:
				err = fmt.Errorf("unsupported edit mode %q", o.EditMode)
			}
			if err != nil {
				return preservedFile(err, results.file, o.ErrOut)
			}

			// Handle all possible errors
			//
			// 1. retryable: propose kubectl replace -f
			// 2. notfound: indicate the location of the saved configuration of the deleted resource
			// 3. invalid: retry those on the spot by looping ie. reloading the editor
			if results.retryable > 0 {
				//retryable 错误表示该操作可以重试，用户可以使用 kubectl replace -f <file> 来再次尝试。
				fmt.Fprintf(o.ErrOut, "You can run `%s replace -f %s` to try this update again.\n", filepath.Base(os.Args[0]), file)
				return cmdutil.ErrExit
			}
			if results.notfound > 0 {
				//notfound 错误表示用户编辑的资源已经被删除，编辑内容被保存到指定的文件。
				fmt.Fprintf(o.ErrOut, "The edits you made on deleted resources have been saved to %q\n", file)
				return cmdutil.ErrExit
			}

			if len(results.edit) == 0 {
				//如果没有进行有效的编辑（results.edit 长度为 0），并且没有删除资源的情况，删除临时编辑文件。
				//如果编辑的资源已删除，提示用户编辑内容已保存。
				if results.notfound == 0 {
					os.Remove(file)
				} else {
					fmt.Fprintf(o.Out, "The edits you made on deleted resources have been saved to %q\n", file)
				}
				return nil
			}

			if len(results.header.reasons) > 0 {
				//检查是否有错误原因，如果有，设置 containsError 为 true。
				containsError = true
			}
		}
	}

	switch o.EditMode {
	// If doing normal edit we cannot use Visit because we need to edit a list for convenience. Ref: #20519
	case NormalEditMode:
		infos, err := o.OriginalResult.Infos()
		if err != nil {
			return err
		}
		if len(infos) == 0 {
			return errors.New("edit cancelled, no objects found")
		}
		return editFn(infos)
	case ApplyEditMode:
		infos, err := o.OriginalResult.Infos()
		if err != nil {
			return err
		}
		var annotationInfos []*resource.Info
		for i := range infos {
			data, err := util.GetOriginalConfiguration(infos[i].Object)
			if err != nil {
				return err
			}
			if data == nil {
				continue
			}

			tempInfos, err := o.updatedResultGetter(data).Infos()
			if err != nil {
				return err
			}
			annotationInfos = append(annotationInfos, tempInfos[0])
		}
		if len(annotationInfos) == 0 {
			return errors.New("no last-applied-configuration annotation found on resources, to create the annotation, use command `kubectl apply set-last-applied --create-annotation`")
		}
		return editFn(annotationInfos)
	// If doing an edit before created, we don't want a list and instead want the normal behavior as kubectl create.
	case EditBeforeCreateMode:
		return o.OriginalResult.Visit(func(info *resource.Info, err error) error {
			return editFn([]*resource.Info{info})
		})
	default:
		return fmt.Errorf("unsupported edit mode %q", o.EditMode)
	}
}

func (o *EditOptions) extractManagedFields(obj runtime.Object) error {
	o.managedFields = make(map[types.UID][]metav1.ManagedFieldsEntry)
	if meta.IsListType(obj) {
		err := meta.EachListItem(obj, func(obj runtime.Object) error {
			uid, mf, err := clearManagedFields(obj)
			if err != nil {
				return err
			}
			o.managedFields[uid] = mf
			return nil
		})
		return err
	}
	uid, mf, err := clearManagedFields(obj)
	if err != nil {
		return err
	}
	o.managedFields[uid] = mf
	return nil
}

func clearManagedFields(obj runtime.Object) (types.UID, []metav1.ManagedFieldsEntry, error) {
	metaObjs, err := meta.Accessor(obj)
	if err != nil {
		return "", nil, err
	}
	mf := metaObjs.GetManagedFields()
	metaObjs.SetManagedFields(nil)
	return metaObjs.GetUID(), mf, nil
}

func (o *EditOptions) restoreManagedFields(infos []*resource.Info) error {
	for _, info := range infos {
		metaObjs, err := meta.Accessor(info.Object)
		if err != nil {
			return err
		}
		mf := o.managedFields[metaObjs.GetUID()]
		metaObjs.SetManagedFields(mf)
	}
	return nil
}

func (o *EditOptions) visitToApplyEditPatch(originalInfos []*resource.Info, patchVisitor resource.Visitor) error {
	err := patchVisitor.Visit(func(info *resource.Info, incomingErr error) error {
		editObjUID, err := meta.NewAccessor().UID(info.Object)
		if err != nil {
			return err
		}

		var originalInfo *resource.Info
		for _, i := range originalInfos {
			originalObjUID, err := meta.NewAccessor().UID(i.Object)
			if err != nil {
				return err
			}
			if editObjUID == originalObjUID {
				originalInfo = i
				break
			}
		}
		if originalInfo == nil {
			return fmt.Errorf("no original object found for %#v", info.Object)
		}

		originalJS, err := encodeToJSON(originalInfo.Object.(runtime.Unstructured))
		if err != nil {
			return err
		}

		editedJS, err := encodeToJSON(info.Object.(runtime.Unstructured))
		if err != nil {
			return err
		}

		if reflect.DeepEqual(originalJS, editedJS) {
			printer, err := o.ToPrinter("skipped")
			if err != nil {
				return err
			}
			return printer.PrintObj(info.Object, o.Out)
		}
		err = o.annotationPatch(info)
		if err != nil {
			return err
		}

		printer, err := o.ToPrinter("edited")
		if err != nil {
			return err
		}
		return printer.PrintObj(info.Object, o.Out)
	})
	return err
}

func (o *EditOptions) annotationPatch(update *resource.Info) error {
	patch, _, patchType, err := GetApplyPatch(update.Object.(runtime.Unstructured))
	if err != nil {
		return err
	}
	mapping := update.ResourceMapping()
	client, err := o.f.UnstructuredClientForMapping(mapping)
	if err != nil {
		return err
	}
	helper := resource.NewHelper(client, mapping).
		WithFieldManager(o.FieldManager).
		WithFieldValidation(o.ValidationDirective).
		WithSubresource(o.Subresource)
	_, err = helper.Patch(o.CmdNamespace, update.Name, patchType, patch, nil)
	return err
}

// GetApplyPatch is used to get and apply patches
func GetApplyPatch(obj runtime.Unstructured) ([]byte, []byte, types.PatchType, error) {
	beforeJSON, err := encodeToJSON(obj)
	if err != nil {
		return nil, []byte(""), types.MergePatchType, err
	}
	objCopy := obj.DeepCopyObject()
	accessor := meta.NewAccessor()
	annotations, err := accessor.Annotations(objCopy)
	if err != nil {
		return nil, beforeJSON, types.MergePatchType, err
	}
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[corev1.LastAppliedConfigAnnotation] = string(beforeJSON)
	accessor.SetAnnotations(objCopy, annotations)
	afterJSON, err := encodeToJSON(objCopy.(runtime.Unstructured))
	if err != nil {
		return nil, beforeJSON, types.MergePatchType, err
	}
	patch, err := jsonpatch.CreateMergePatch(beforeJSON, afterJSON)
	return patch, beforeJSON, types.MergePatchType, err
}

func encodeToJSON(obj runtime.Unstructured) ([]byte, error) {
	serialization, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	if err != nil {
		return nil, err
	}
	js, err := yaml.ToJSON(serialization)
	if err != nil {
		return nil, err
	}
	return js, nil
}

// originalInfos: 原始对象列表，即用户编辑前的 Kubernetes 资源信息。
//
// patchVisitor: 访问编辑后的对象，用于遍历用户修改后的资源对象。
//
// results: 存储执行结果，包括错误信息
// ，用于处理 Kubernetes 资源的 编辑后提交（patch） 逻辑。它的作用是对比原始对象和编辑后的对象，计算出差异，并根据不同情况使用 MergePatch 或 StrategicMergePatch 方式进行 PATCH 更新。
func (o *EditOptions) visitToPatch(originalInfos []*resource.Info, patchVisitor resource.Visitor, results *editResults) error {
	err := patchVisitor.Visit(func(info *resource.Info, incomingErr error) error {
		//通过 UID 查找对应的原始对象 originalInfo，确保是同一个资源。
		editObjUID, err := meta.NewAccessor().UID(info.Object)
		if err != nil {
			return err
		}

		var originalInfo *resource.Info
		//通过 UID 查找对应的原始对象 originalInfo，确保是同一个资源。
		for _, i := range originalInfos {
			originalObjUID, err := meta.NewAccessor().UID(i.Object)
			if err != nil {
				return err
			}
			if editObjUID == originalObjUID {
				originalInfo = i
				break
			}
		}
		if originalInfo == nil {
			return fmt.Errorf("no original object found for %#v", info.Object)
		}
		// 将Object interface转成 runtime.Unstructured interface
		originalJS, err := encodeToJSON(originalInfo.Object.(runtime.Unstructured))
		if err != nil {
			return err
		}

		editedJS, err := encodeToJSON(info.Object.(runtime.Unstructured))
		if err != nil {
			return err
		}

		if reflect.DeepEqual(originalJS, editedJS) {
			// no edit, so just skip it.
			//把原始和编辑后的对象转换成 JSON，如果两者相同，则跳过更新。
			printer, err := o.ToPrinter("skipped")
			if err != nil {
				return err
			}
			return printer.PrintObj(info.Object, o.Out)
		}

		preconditions := []mergepatch.PreconditionFunc{
			mergepatch.RequireKeyUnchanged("apiVersion"),
			mergepatch.RequireKeyUnchanged("kind"),
			mergepatch.RequireMetadataKeyUnchanged("name"),
			mergepatch.RequireKeyUnchanged("managedFields"),
		}
		versionedObject, err := scheme.Scheme.New(info.Mapping.GroupVersionKind)
		var patchType types.PatchType
		var patch []byte
		switch {
		case runtime.IsNotRegisteredError(err):
			// fall back to generic JSON merge patch
			//如果对象 未在 Kubernetes Scheme 注册，则使用 JSON Merge Patch。
			//MergePatch 直接计算 JSON 差异，而 StrategicMergePatch 允许更复杂的策略，如合并数组字段。
			patchType = types.MergePatchType
			patch, err = jsonpatch.CreateMergePatch(originalJS, editedJS)
			if err != nil {
				klog.V(4).Infof("Unable to calculate diff, no merge is possible: %v", err)
				return err
			}
			var patchMap map[string]interface{}
			err = json.Unmarshal(patch, &patchMap)
			if err != nil {
				klog.V(4).Infof("Unable to calculate diff, no merge is possible: %v", err)
				return err
			}
			for _, precondition := range preconditions {
				if !precondition(patchMap) {
					klog.V(4).Infof("Unable to calculate diff, no merge is possible: %v", err)
					return fmt.Errorf("%s", "At least one of apiVersion, kind and name was changed")
				}
			}
		case err != nil:
			return err
		default:
			//否则使用 Strategic Merge Patch（针对 Kubernetes 资源结构更智能的 Patch 方式）。
			patchType = types.StrategicMergePatchType
			patch, err = strategicpatch.CreateTwoWayMergePatch(originalJS, editedJS, versionedObject, preconditions...)
			if err != nil {
				klog.V(4).Infof("Unable to calculate diff, no merge is possible: %v", err)
				if mergepatch.IsPreconditionFailed(err) {
					return fmt.Errorf("%s", "At least one of apiVersion, kind and name was changed")
				}
				return err
			}
		}

		if o.OutputPatch {
			fmt.Fprintf(o.Out, "Patch: %s\n", string(patch))
		}
		//使用 Kubernetes 客户端 发送 PATCH 请求，将 patch 应用到资源。
		patched, err := resource.NewHelper(info.Client, info.Mapping).
			WithFieldManager(o.FieldManager).
			WithFieldValidation(o.ValidationDirective).
			WithSubresource(o.Subresource).
			Patch(info.Namespace, info.Name, patchType, patch, nil)
		if err != nil {
			fmt.Fprintln(o.ErrOut, results.addError(err, info))
			return nil
		}
		//打印 PATCH 结果。
		info.Refresh(patched, true)
		printer, err := o.ToPrinter("edited")
		if err != nil {
			return err
		}
		return printer.PrintObj(info.Object, o.Out)
	})
	return err
}

func (o *EditOptions) visitToCreate(createVisitor resource.Visitor) error {
	err := createVisitor.Visit(func(info *resource.Info, incomingErr error) error {
		obj, err := resource.NewHelper(info.Client, info.Mapping).
			WithFieldManager(o.FieldManager).
			WithFieldValidation(o.ValidationDirective).
			Create(info.Namespace, true, info.Object)
		if err != nil {
			return err
		}
		info.Refresh(obj, true)
		printer, err := o.ToPrinter("created")
		if err != nil {
			return err
		}
		return printer.PrintObj(info.Object, o.Out)
	})
	return err
}

func (o *EditOptions) visitAnnotation(annotationVisitor resource.Visitor) error {
	// iterate through all items to apply annotations
	err := annotationVisitor.Visit(func(info *resource.Info, incomingErr error) error {
		// put configuration annotation in "updates"
		if o.ApplyAnnotation {
			//o.ApplyAnnotation 为 true 时：
			//util.CreateOrUpdateAnnotation(...) 负责给 info.Object（Kubernetes 资源）添加或更新注解，比如：
			//metadata:
			//  annotations:
			//    kubectl.kubernetes.io/last-applied-configuration: "{...}"  # 存储 `kubectl edit` 修改后的 JSON
			//scheme.DefaultJSONEncoder() 用于 JSON 编码资源。
			if err := util.CreateOrUpdateAnnotation(true, info.Object, scheme.DefaultJSONEncoder()); err != nil {
				return err
			}
		}
		//o.Recorder.Record(info.Object) 负责记录 kubectl edit 这次操作的历史信息。
		//记录失败不会影响流程，只是输出日志
		if err := o.Recorder.Record(info.Object); err != nil {
			klog.V(4).Infof("error recording current command: %v", err)
		}

		return nil

	})
	return err
}

// EditMode can be either NormalEditMode, EditBeforeCreateMode or ApplyEditMode
type EditMode string

const (
	// NormalEditMode is an edit mode
	NormalEditMode EditMode = "normal_mode"

	// EditBeforeCreateMode is an edit mode
	EditBeforeCreateMode EditMode = "edit_before_create_mode"

	// ApplyEditMode is an edit mode
	ApplyEditMode EditMode = "edit_last_applied_mode"
)

// editReason preserves a message about the reason this file must be edited again
type editReason struct {
	head  string
	other []string
}

// editHeader includes a list of reasons the edit must be retried
type editHeader struct {
	reasons []editReason
}

// writeTo outputs the current header information into a stream
func (h *editHeader) writeTo(w io.Writer, editMode EditMode) error {
	if editMode == ApplyEditMode {
		fmt.Fprint(w, `# Please edit the 'last-applied-configuration' annotations below.
# Lines beginning with a '#' will be ignored, and an empty file will abort the edit.
#
`)
	} else {
		fmt.Fprint(w, `# Please edit the object below. Lines beginning with a '#' will be ignored,
# and an empty file will abort the edit. If an error occurs while saving this file will be
# reopened with the relevant failures.
#
`)
	}

	for _, r := range h.reasons {
		if len(r.other) > 0 {
			fmt.Fprintf(w, "# %s:\n", hashOnLineBreak(r.head))
		} else {
			fmt.Fprintf(w, "# %s\n", hashOnLineBreak(r.head))
		}
		for _, o := range r.other {
			fmt.Fprintf(w, "# * %s\n", hashOnLineBreak(o))
		}
		fmt.Fprintln(w, "#")
	}
	return nil
}

// editResults capture the result of an update
type editResults struct {
	header    editHeader
	retryable int
	notfound  int
	edit      []*resource.Info
	file      string
}

func (r *editResults) addError(err error, info *resource.Info) string {
	resourceString := info.Mapping.Resource.Resource
	if len(info.Mapping.Resource.Group) > 0 {
		resourceString = resourceString + "." + info.Mapping.Resource.Group
	}

	switch {
	case apierrors.IsInvalid(err):
		r.edit = append(r.edit, info)
		reason := editReason{
			head: fmt.Sprintf("%s %q was not valid", resourceString, info.Name),
		}
		if err, ok := err.(apierrors.APIStatus); ok {
			if details := err.Status().Details; details != nil {
				for _, cause := range details.Causes {
					reason.other = append(reason.other, fmt.Sprintf("%s: %s", cause.Field, cause.Message))
				}
			}
		}
		r.header.reasons = append(r.header.reasons, reason)
		return fmt.Sprintf("error: %s %q is invalid", resourceString, info.Name)
	case apierrors.IsNotFound(err):
		r.notfound++
		return fmt.Sprintf("error: %s %q could not be found on the server", resourceString, info.Name)
	default:
		r.retryable++
		return fmt.Sprintf("error: %s %q could not be patched: %v", resourceString, info.Name, err)
	}
}

// preservedFile writes out a message about the provided file if it exists to the
// provided output stream when an error happens. Used to notify the user where
// their updates were preserved.
func preservedFile(err error, path string, out io.Writer) error {
	if len(path) > 0 {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			fmt.Fprintf(out, "A copy of your changes has been stored to %q\n", path)
		}
	}
	return err
}

// hasLines returns true if any line in the provided stream is non empty - has non-whitespace
// characters, or the first non-whitespace character is a '#' indicating a comment. Returns
// any errors encountered reading the stream.
func hasLines(r io.Reader) (bool, error) {
	// TODO: if any files we read have > 64KB lines, we'll need to switch to bytes.ReadLine
	// TODO: probably going to be secrets
	s := bufio.NewScanner(r)
	for s.Scan() {
		if line := strings.TrimSpace(s.Text()); len(line) > 0 && line[0] != '#' {
			return true, nil
		}
	}
	if err := s.Err(); err != nil && err != io.EOF {
		return false, err
	}
	return false, nil
}

// hashOnLineBreak returns a string built from the provided string by inserting any necessary '#'
// characters after '\n' characters, indicating a comment.
func hashOnLineBreak(s string) string {
	r := ""
	for i, ch := range s {
		j := i + 1
		if j < len(s) && ch == '\n' && s[j] != '#' {
			r += "\n# "
		} else {
			r += string(ch)
		}
	}
	return r
}

// editorEnvs returns an ordered list of env vars to check for editor preferences.
func editorEnvs() []string {
	return []string{
		"KUBE_EDITOR",
		"EDITOR",
	}
}
