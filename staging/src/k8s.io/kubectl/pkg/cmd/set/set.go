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

package set

import (
	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	setLong = templates.LongDesc(i18n.T(`
		Configure application resources.

		These commands help you make changes to existing application resources.`))
)

// NewCmdSet returns an initialized Command instance for 'set' sub command
// 示例： 更新名为 my-deployment 的 nginx 容器的镜像为 nginx:1.16：
// kubectl set image deployment/my-deployment nginx=nginx:1.16
func NewCmdSet(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "set SUBCOMMAND",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Set specific features on objects"),
		Long:                  setLong,
		Run:                   cmdutil.DefaultSubCommandRun(streams.ErrOut),
	}

	// add subcommands
	cmd.AddCommand(NewCmdImage(f, streams))          //kubectl set image deployment/my-deployment nginx=nginx:1.16
	cmd.AddCommand(NewCmdResources(f, streams))      //kubectl set resources deployment/my-deployment --limits=cpu=1,memory=512Mi
	cmd.AddCommand(NewCmdSelector(f, streams))       //kubectl set selector service/my-service environment=production
	cmd.AddCommand(NewCmdSubject(f, streams))        //kubectl set subject rolebinding my-rolebinding --user=john
	cmd.AddCommand(NewCmdServiceAccount(f, streams)) //kubectl set serviceaccount deployment my-deployment my-serviceaccount
	cmd.AddCommand(NewCmdEnv(f, streams))            //kubectl set env deployment/my-deployment FOO=bar

	return cmd
}
