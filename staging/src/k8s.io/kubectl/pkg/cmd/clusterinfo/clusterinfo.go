/*
Copyright 2015 The Kubernetes Authors.

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

package clusterinfo

import (
	"fmt"
	"io"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	restclient "k8s.io/client-go/rest"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/spf13/cobra"
)

var (
	longDescr = templates.LongDesc(i18n.T(`
  Display addresses of the control plane and services with label kubernetes.io/cluster-service=true.
  To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.`))

	clusterinfoExample = templates.Examples(i18n.T(`
		# Print the address of the control plane and cluster services
		kubectl cluster-info`))
)

type ClusterInfoOptions struct {
	genericiooptions.IOStreams

	Namespace string

	Builder *resource.Builder
	Client  *restclient.Config
}

// 1. 查看集群信息
// kubectl cluster-info
// 输出示例：
// Kubernetes control plane is running at https://192.168.1.100:6443
// CoreDNS is running at https://192.168.1.100:6443/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy
// 这个命令会显示 API Server 和一些关键服务（如 CoreDNS）的运行状态和地址。
//
// 2. 获取详细的集群调试信息
// kubectl cluster-info dump
// 这个命令会收集更详细的诊断信息，包括：
// Pod、Service、Node、ConfigMap 等资源的 YAML 描述。
// 日志信息（如果可用）。
// 事件信息（如 kubectl get events）。
// 适用于调试 Kubernetes 集群的问题。
func NewCmdClusterInfo(restClientGetter genericclioptions.RESTClientGetter, ioStreams genericiooptions.IOStreams) *cobra.Command {
	o := &ClusterInfoOptions{
		IOStreams: ioStreams,
	}

	cmd := &cobra.Command{
		Use:     "cluster-info",
		Short:   i18n.T("Display cluster information"),
		Long:    longDescr,
		Example: clusterinfoExample,
		Run: func(cmd *cobra.Command, args []string) {
			cmdutil.CheckErr(o.Complete(restClientGetter, cmd))
			cmdutil.CheckErr(o.Run())
		},
	}
	cmd.AddCommand(NewCmdClusterInfoDump(restClientGetter, ioStreams))
	return cmd
}

func (o *ClusterInfoOptions) Complete(restClientGetter genericclioptions.RESTClientGetter, cmd *cobra.Command) error {
	var err error
	o.Client, err = restClientGetter.ToRESTConfig()
	if err != nil {
		return err
	}

	cmdNamespace := cmdutil.GetFlagString(cmd, "namespace")
	if cmdNamespace == "" {
		cmdNamespace = metav1.NamespaceSystem
	}
	o.Namespace = cmdNamespace

	o.Builder = resource.NewBuilder(restClientGetter)
	return nil
}

func (o *ClusterInfoOptions) Run() error {
	// TODO use generalized labels once they are implemented (#341)
	b := o.Builder.
		WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
		NamespaceParam(o.Namespace).DefaultNamespace().
		LabelSelectorParam("kubernetes.io/cluster-service=true"). //筛选带有 kubernetes.io/cluster-service=true 标签的 Service，即集群关键服务（如 CoreDNS）。
		ResourceTypeOrNameArgs(false, []string{"services"}...).
		Latest()
	//b.Do() 执行查询。
	//Visit() 遍历所有匹配的 Service 资源。
	//printService(o.Out, "Kubernetes control plane", o.Client.Host) 打印 API Server 地址。
	err := b.Do().Visit(func(r *resource.Info, err error) error {
		if err != nil {
			return err
		}
		printService(o.Out, "Kubernetes control plane", o.Client.Host)

		services := r.Object.(*corev1.ServiceList).Items
		for _, service := range services {
			var link string
			if len(service.Status.LoadBalancer.Ingress) > 0 {
				//获取 ServiceList 资源，遍历所有服务。
				//如果 Service 通过 LoadBalancer 方式暴露，则：
				//取 ingress.IP 或 ingress.Hostname。
				//构造 http://IP:端口 访问地址。
				ingress := service.Status.LoadBalancer.Ingress[0]
				ip := ingress.IP
				if ip == "" {
					ip = ingress.Hostname
				}
				for _, port := range service.Spec.Ports {
					link += "http://" + ip + ":" + strconv.Itoa(int(port.Port)) + " "
				}
			} else {
				name := service.ObjectMeta.Name

				if len(service.Spec.Ports) > 0 {
					port := service.Spec.Ports[0]

					// guess if the scheme is https
					scheme := ""
					if port.Name == "https" || port.Port == 443 {
						scheme = "https"
					}

					// format is <scheme>:<service-name>:<service-port-name>
					name = utilnet.JoinSchemeNamePort(scheme, service.ObjectMeta.Name, port.Name)
				}
				//通过 kubectl proxy 方式访问 API 代理地址：
				//https://<API_SERVER>/api/v1/namespaces/<namespace>/services/<service>/proxy
				if len(o.Client.GroupVersion.Group) == 0 {
					link = o.Client.Host + "/api/" + o.Client.GroupVersion.Version + "/namespaces/" + service.ObjectMeta.Namespace + "/services/" + name + "/proxy"
				} else {
					link = o.Client.Host + "/api/" + o.Client.GroupVersion.Group + "/" + o.Client.GroupVersion.Version + "/namespaces/" + service.ObjectMeta.Namespace + "/services/" + name + "/proxy"

				}
			}
			//kubernetes.io/name 标签存的是 Service 的可读名称，如果没有，就用 metadata.name。
			//printService() 输出 Service 名称和访问地址。
			name := service.ObjectMeta.Labels["kubernetes.io/name"]
			if len(name) == 0 {
				name = service.ObjectMeta.Name
			}
			printService(o.Out, name, link)
		}
		return nil
	})
	o.Out.Write([]byte("\nTo further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.\n"))
	return err

	// TODO consider printing more information about cluster
}

func printService(out io.Writer, name, link string) {
	fmt.Fprint(out, name)
	fmt.Fprint(out, " is running at ")
	fmt.Fprint(out, link)
	fmt.Fprintln(out, "")
}
