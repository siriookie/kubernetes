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

package util

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	openapiclient "k8s.io/client-go/openapi"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/util/openapi"
	"k8s.io/kubectl/pkg/validation"
)

// Factory provides abstractions that allow the Kubectl command to be extended across multiple types
// of resources and different API sets.
// The rings are here for a reason. In order for composers to be able to provide alternative factory implementations
// they need to provide low level pieces of *certain* functions so that when the factory calls back into itself
// it uses the custom version of the function. Rather than try to enumerate everything that someone would want to override
// we split the factory into rings, where each ring can depend on methods in an earlier ring, but cannot depend
// upon peer methods in its own ring.
// TODO: make the functions interfaces
// TODO: pass the various interfaces on the factory directly into the command constructors (so the
// commands are decoupled from the factory).
// 主要用于 抽象出访问 Kubernetes 资源的各种方式，从而允许 kubectl 命令支持多种 API 资源和不同的 Kubernetes 集群。
type Factory interface {
	genericclioptions.RESTClientGetter

	// DynamicClient returns a dynamic client ready for use
	//返回 dynamic.Interface，用于动态操作 Kubernetes 资源。
	//
	//dynamic.Interface 允许 kubectl 操作未知的自定义资源（CRD）
	DynamicClient() (dynamic.Interface, error)

	// KubernetesClientSet gives you back an external clientset
	//返回 kubernetes.Clientset，它是 client-go 提供的标准 Kubernetes API 客户端，可以用于：
	//
	//访问 Pod、Deployment、Service 等标准资源。
	//
	//适用于 已知 API 资源。
	KubernetesClientSet() (*kubernetes.Clientset, error)
	//返回 RESTClient，用于发送 RESTful API 请求。
	// Returns a RESTClient for accessing Kubernetes resources or an error.
	RESTClient() (*restclient.RESTClient, error)

	// NewBuilder returns an object that assists in loading objects from both disk and the server
	// and which implements the common patterns for CLI interactions with generic resources.
	//用于加载 Kubernetes 资源：
	//
	//从 文件（如 YAML/JSON 配置文件）。
	//
	//从 集群（通过 kubectl get 读取）。
	NewBuilder() *resource.Builder

	// Returns a RESTClient for working with the specified RESTMapping or an error. This is intended
	// for working with arbitrary resources and is not guaranteed to point to a Kubernetes APIServer.
	//根据 RESTMapping 获取对应的 RESTClient。
	//
	//主要用于未知的 API 资源，如 CRD。
	ClientForMapping(mapping *meta.RESTMapping) (resource.RESTClient, error)
	// Returns a RESTClient for working with Unstructured objects.
	//返回一个 RESTClient，用于操作 Unstructured 类型的对象。
	//
	//Unstructured 适用于动态资源（即 kubectl get 查询时，资源类型不固定）。
	UnstructuredClientForMapping(mapping *meta.RESTMapping) (resource.RESTClient, error)
	//返回一个 Schema，用于校验本地 YAML/JSON 文件，确保格式正确。
	// Returns a schema that can validate objects stored on disk.
	Validator(validationDirective string) (validation.Schema, error)

	// Used for retrieving openapi v2 resources.
	//支持 OpenAPI v2 资源解析，用于 Kubernetes API 兼容性检查。
	openapi.OpenAPIResourcesGetter

	// OpenAPIV3Schema returns a client for fetching parsed schemas for
	// any group version
	//返回 OpenAPI v3 客户端，可以用于获取 Kubernetes API 资源的结构定义。
	OpenAPIV3Client() (openapiclient.Client, error)
}
