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

package auth

import (
	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/genericiooptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

// 检查当前用户是否可以创建 Pod
// kubectl auth can-i create pods
// ✅ 输出：
// yes
// 说明当前用户有创建 Pod 的权限。
// ❌ 如果没有权限：
// no

// kubectl auth whoami
// 表示当前身份是 default 命名空间中的 my-sa ServiceAccount。
// 如果当前用户是 admin，可能返回：
// admin
// NewCmdAuth returns an initialized Command instance for 'auth' sub command
//
//system:serviceaccount:default:my-sa
func NewCmdAuth(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	// Parent command to which all subcommands are added.
	cmds := &cobra.Command{
		Use:   "auth",
		Short: "Inspect authorization",
		Long:  `Inspect authorization.`,
		Run:   cmdutil.DefaultSubCommandRun(streams.ErrOut),
	}

	cmds.AddCommand(NewCmdCanI(f, streams))      //检查当前用户是否可以执行某个操作
	cmds.AddCommand(NewCmdReconcile(f, streams)) //将权限对象（如 Role, RoleBinding）与集群状态同步
	cmds.AddCommand(NewCmdWhoAmI(f, streams))    //显示当前身份信息（如用户、组、服务账户）

	return cmds
}
