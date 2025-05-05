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

package endpoints

import (
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"

	restful "github.com/emicklei/go-restful/v3"
	"sigs.k8s.io/structured-merge-diff/v4/fieldpath"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/endpoints/deprecation"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	"k8s.io/apiserver/pkg/endpoints/handlers"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	utilwarning "k8s.io/apiserver/pkg/endpoints/warning"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storageversion"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	versioninfo "k8s.io/component-base/version"
)

const (
	RouteMetaGVK              = "x-kubernetes-group-version-kind"
	RouteMetaSelectableFields = "x-kubernetes-selectable-fields"
	RouteMetaAction           = "x-kubernetes-action"
)

type APIInstaller struct {
	group             *APIGroupVersion
	prefix            string // Path prefix where API resources are to be registered.
	minRequestTimeout time.Duration
}

// Struct capturing information about an action ("GET", "POST", "WATCH", "PROXY", etc).
type action struct {
	Verb   string               // Verb identifying the action ("GET", "POST", "WATCH", "PROXY", etc).
	Path   string               // The path of the action
	Params []*restful.Parameter // List of parameters associated with the action.
	Namer  handlers.ScopeNamer
	//action 结构体中这个字段的意思是：
	//
	//是否允许通过 /api/v1/<resource> 这个路径访问 所有命名空间下的资源。这个字段仅在 resource 是 namespaced 资源 时有意义。
	//
	//即：
	//字段	含义
	//AllNamespaces: true	表示允许在 所有命名空间 范围执行该操作（如 /api/v1/pods）。
	//AllNamespaces: false	该操作只能在具体命名空间路径下执行（如 /api/v1/namespaces/default/pods）。
	//
	//但是对于 非 namespaced（cluster-scoped）资源，如 Node：
	//
	//根本没有命名空间的概念；
	//
	//所以 /api/v1/nodes 就是列出全部节点，不存在“跨命名空间”问题；
	//
	//所以这个字段在 cluster-scoped 资源上是 无意义的。
	AllNamespaces bool // true iff the action is namespaced but works on aggregate result for all namespaces
}

func ConvertGroupVersionIntoToDiscovery(list []metav1.APIResource) ([]apidiscoveryv2.APIResourceDiscovery, error) {
	var apiResourceList []apidiscoveryv2.APIResourceDiscovery
	parentResources := make(map[string]int)

	// Loop through all top-level resources
	for _, r := range list {
		if strings.Contains(r.Name, "/") {
			// Skip subresources for now so we can get the list of resources
			continue
		}

		var scope apidiscoveryv2.ResourceScope
		if r.Namespaced {
			scope = apidiscoveryv2.ScopeNamespace
		} else {
			scope = apidiscoveryv2.ScopeCluster
		}

		resource := apidiscoveryv2.APIResourceDiscovery{
			Resource: r.Name,
			Scope:    scope,
			ResponseKind: &metav1.GroupVersionKind{
				Group:   r.Group,
				Version: r.Version,
				Kind:    r.Kind,
			},
			Verbs:            r.Verbs,
			ShortNames:       r.ShortNames,
			Categories:       r.Categories,
			SingularResource: r.SingularName,
		}
		apiResourceList = append(apiResourceList, resource)
		parentResources[r.Name] = len(apiResourceList) - 1
	}

	// Loop through all subresources
	for _, r := range list {
		// Split resource name and subresource name
		split := strings.SplitN(r.Name, "/", 2)

		if len(split) != 2 {
			// Skip parent resources
			continue
		}

		var scope apidiscoveryv2.ResourceScope
		if r.Namespaced {
			scope = apidiscoveryv2.ScopeNamespace
		} else {
			scope = apidiscoveryv2.ScopeCluster
		}

		parentidx, exists := parentResources[split[0]]
		if !exists {
			// If a subresource exists without a parent, create a parent
			apiResourceList = append(apiResourceList, apidiscoveryv2.APIResourceDiscovery{
				Resource: split[0],
				Scope:    scope,
				// avoid nil panics in v0.26.0-v0.26.3 client-go clients
				// see https://github.com/kubernetes/kubernetes/issues/118361
				ResponseKind: &metav1.GroupVersionKind{},
			})
			parentidx = len(apiResourceList) - 1
			parentResources[split[0]] = parentidx
		}

		if apiResourceList[parentidx].Scope != scope {
			return nil, fmt.Errorf("Error: Parent %s (scope: %s) and subresource %s (scope: %s) scope do not match", split[0], apiResourceList[parentidx].Scope, split[1], scope)
			//
		}

		subresource := apidiscoveryv2.APISubresourceDiscovery{
			Subresource: split[1],
			Verbs:       r.Verbs,
			// avoid nil panics in v0.26.0-v0.26.3 client-go clients
			// see https://github.com/kubernetes/kubernetes/issues/118361
			ResponseKind: &metav1.GroupVersionKind{},
		}
		if r.Kind != "" {
			subresource.ResponseKind = &metav1.GroupVersionKind{
				Group:   r.Group,
				Version: r.Version,
				Kind:    r.Kind,
			}
		}
		apiResourceList[parentidx].Subresources = append(apiResourceList[parentidx].Subresources, subresource)
	}

	return apiResourceList, nil
}

// An interface to see if one storage supports override its default verb for monitoring
type StorageMetricsOverride interface {
	// OverrideMetricsVerb gives a storage object an opportunity to override the verb reported to the metrics endpoint
	OverrideMetricsVerb(oldVerb string) (newVerb string)
}

// An interface to see if an object supports swagger documentation as a method
type documentable interface {
	SwaggerDoc() map[string]string
}

// toDiscoveryKubeVerb maps an action.Verb to the logical kube verb, used for discovery
var toDiscoveryKubeVerb = map[string]string{
	"CONNECT":          "", // do not list in discovery.
	"DELETE":           "delete",
	"DELETECOLLECTION": "deletecollection",
	"GET":              "get",
	"LIST":             "list",
	"PATCH":            "patch",
	"POST":             "create",
	"PROXY":            "proxy",
	"PUT":              "update",
	"WATCH":            "watch",
	"WATCHLIST":        "watch",
}

// Install handlers for API resources.
func (a *APIInstaller) Install() ([]metav1.APIResource, []*storageversion.ResourceInfo, *restful.WebService, []error) {
	var apiResources []metav1.APIResource
	var resourceInfos []*storageversion.ResourceInfo
	var errors []error
	ws := a.newWebService()

	// Register the paths in a deterministic (sorted) order to get a deterministic swagger spec.
	paths := make([]string, len(a.group.Storage))
	var i int
	//a.group.Storage：是一个 map[string]rest.Storage，键是资源路径（如 "pods"），值是对应的存储后端。
	for path := range a.group.Storage {
		paths[i] = path
		i++
	}
	sort.Strings(paths)
	for _, path := range paths {
		apiResource, resourceInfo, err := a.registerResourceHandlers(path, a.group.Storage[path], ws)
		if err != nil {
			errors = append(errors, fmt.Errorf("error in registering resource: %s, %v", path, err))
		}
		if apiResource != nil {
			apiResources = append(apiResources, *apiResource)
		}
		if resourceInfo != nil {
			resourceInfos = append(resourceInfos, resourceInfo)
		}
	}
	return apiResources, resourceInfos, ws, errors
}

// newWebService creates a new restful webservice with the api installer's prefix and version.
func (a *APIInstaller) newWebService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path(a.prefix)
	// a.prefix contains "prefix/group/version"
	ws.Doc("API at " + a.prefix)
	// Backwards compatibility, we accepted objects with empty content-type at V1.
	// If we stop using go-restful, we can default empty content-type to application/json on an
	// endpoint by endpoint basis
	ws.Consumes("*/*")
	mediaTypes, streamMediaTypes := negotiation.MediaTypesForSerializer(a.group.Serializer)
	ws.Produces(append(mediaTypes, streamMediaTypes...)...)
	ws.ApiVersion(a.group.GroupVersion.String())

	return ws
}

// calculate the storage gvk, the gvk objects are converted to before persisted to the etcd.
func getStorageVersionKind(storageVersioner runtime.GroupVersioner, storage rest.Storage, typer runtime.ObjectTyper) (schema.GroupVersionKind, error) {
	object := storage.New()
	fqKinds, _, err := typer.ObjectKinds(object)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	gvk, ok := storageVersioner.KindForGroupVersionKinds(fqKinds)
	if !ok {
		return schema.GroupVersionKind{}, fmt.Errorf("cannot find the storage version kind for %v", reflect.TypeOf(object))
	}
	return gvk, nil
}

// GetResourceKind returns the external group version kind registered for the given storage
// object. If the storage object is a subresource and has an override supplied for it, it returns
// the group version kind supplied in the override.
func GetResourceKind(groupVersion schema.GroupVersion, storage rest.Storage, typer runtime.ObjectTyper) (schema.GroupVersionKind, error) {
	// Let the storage tell us exactly what GVK it has
	if gvkProvider, ok := storage.(rest.GroupVersionKindProvider); ok {
		return gvkProvider.GroupVersionKind(groupVersion), nil
	}

	object := storage.New()
	fqKinds, _, err := typer.ObjectKinds(object)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}

	// a given go type can have multiple potential fully qualified kinds.  Find the one that corresponds with the group
	// we're trying to register here
	fqKindToRegister := schema.GroupVersionKind{}
	for _, fqKind := range fqKinds {
		if fqKind.Group == groupVersion.Group {
			fqKindToRegister = groupVersion.WithKind(fqKind.Kind)
			break
		}
	}
	if fqKindToRegister.Empty() {
		return schema.GroupVersionKind{}, fmt.Errorf("unable to locate fully qualified kind for %v: found %v when registering for %v", reflect.TypeOf(object), fqKinds, groupVersion)
	}

	// group is guaranteed to match based on the check above
	return fqKindToRegister, nil
}

func (a *APIInstaller) registerResourceHandlers(path string, storage rest.Storage, ws *restful.WebService) (*metav1.APIResource, *storageversion.ResourceInfo, error) {
	admit := a.group.Admit
	//获取准入控制处理器和 API 版本信息
	//分离主资源和子资源路径（如 pods/status 分离为 pods 和 status）
	optionsExternalVersion := a.group.GroupVersion
	if a.group.OptionsExternalVersion != nil {
		optionsExternalVersion = *a.group.OptionsExternalVersion
	}

	resource, subresource, err := splitSubresource(path)
	if err != nil {
		return nil, nil, err
	}

	group, version := a.group.GroupVersion.Group, a.group.GroupVersion.Version
	//调用 GetResourceKind，从 storage 中推断出资源的 GroupVersionKind（简称 GVK）——比如 apps/v1, Kind=Deployment。
	fqKindToRegister, err := GetResourceKind(a.group.GroupVersion, storage, a.group.Typer)
	if err != nil {
		return nil, nil, err
	}

	versionedPtr, err := a.group.Creater.New(fqKindToRegister)
	if err != nil {
		return nil, nil, err
	}
	defaultVersionedObject := indirectArbitraryPointer(versionedPtr)
	kind := fqKindToRegister.Kind
	isSubresource := len(subresource) > 0

	// If there is a subresource, namespace scoping is defined by the parent resource
	var namespaceScoped bool
	if isSubresource {
		parentStorage, ok := a.group.Storage[resource]
		if !ok {
			return nil, nil, fmt.Errorf("missing parent storage: %q", resource)
		}
		scoper, ok := parentStorage.(rest.Scoper)
		if !ok {
			return nil, nil, fmt.Errorf("%q must implement scoper", resource)
		}
		namespaceScoped = scoper.NamespaceScoped()

	} else {
		scoper, ok := storage.(rest.Scoper)
		if !ok {
			return nil, nil, fmt.Errorf("%q must implement scoper", resource)
		}
		namespaceScoped = scoper.NamespaceScoped()
	}
	//行	接口含义	代表的 HTTP 动作
	//lister	List()	GET（列表）
	//getter	Get()	GET（单个）
	//getterWithOptions	GetWithOptions()	GET + 参数控制
	//gracefulDeleter	支持优雅删除	DELETE
	//collectionDeleter	删除多个资源	DELETE collection
	//updater	Update()	PUT
	//patcher	Patch()	PATCH
	//watcher	Watch()	WATCH
	//connecter	Connect()	CONNECT（如 Exec）
	//storageMeta	提供额外元信息支持	——
	//storageVersionProvider	提供版本信息能力	——
	//gvAcceptor	判断是否接受某版本对象	——
	// what verbs are supported by the storage, used to know what verbs we support per path
	creater, isCreater := storage.(rest.Creater)
	namedCreater, isNamedCreater := storage.(rest.NamedCreater)
	lister, isLister := storage.(rest.Lister)
	getter, isGetter := storage.(rest.Getter)
	getterWithOptions, isGetterWithOptions := storage.(rest.GetterWithOptions)
	gracefulDeleter, isGracefulDeleter := storage.(rest.GracefulDeleter)
	collectionDeleter, isCollectionDeleter := storage.(rest.CollectionDeleter)
	updater, isUpdater := storage.(rest.Updater)
	patcher, isPatcher := storage.(rest.Patcher)
	watcher, isWatcher := storage.(rest.Watcher)
	connecter, isConnecter := storage.(rest.Connecter)
	storageMeta, isMetadata := storage.(rest.StorageMetadata)
	storageVersionProvider, isStorageVersionProvider := storage.(rest.StorageVersionProvider)
	gvAcceptor, _ := storage.(rest.GroupVersionAcceptor)
	//如果 storage 没实现 rest.StorageMetadata 接口，那就用默认的实现 defaultStorageMetadata{}。
	//避免后续使用 storageMeta 时空指针异常。
	if !isMetadata {
		storageMeta = defaultStorageMetadata{}
	}

	if isNamedCreater {
		isCreater = true
	}

	var versionedList interface{}
	if isLister {
		//只有支持列表操作的资源（如 kubectl get pods）才会进入此逻辑。
		list := lister.NewList()
		//通过 Typer（类型解析器）获取该列表对象的 GroupVersionKind（GVK）。
		//例如 PodList 的 GVK 可能是 {Group: "", Version: "v1", Kind: "PodList"}。
		listGVKs, _, err := a.group.Typer.ObjectKinds(list)
		if err != nil {
			return nil, nil, err
		}
		//使用 Creater（对象创建器）根据当前 API 组的版本（如 apps/v1）和列表对象的 Kind（如 PodList）创建一个新的空列表对象。
		//例如：a.group.Creater.New(apps/v1.WithKind("PodList")) 会返回一个 *v1.PodList。
		versionedListPtr, err := a.group.Creater.New(a.group.GroupVersion.WithKind(listGVKs[0].Kind))
		if err != nil {
			return nil, nil, err
		}
		//indirectArbitraryPointer 是一个辅助函数，用于解引用可能的双重指针（如 **v1.PodList → *v1.PodList）。
		//原因：某些对象的创建器可能会返回指针的指针，需要统一处理为单层指针。
		versionedList = indirectArbitraryPointer(versionedListPtr)
	}
	// 创建对应的空对象
	versionedListOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("ListOptions"))
	if err != nil {
		return nil, nil, err
	}
	versionedCreateOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("CreateOptions"))
	if err != nil {
		return nil, nil, err
	}
	versionedPatchOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("PatchOptions"))
	if err != nil {
		return nil, nil, err
	}
	versionedUpdateOptions, err := a.group.Creater.New(optionsExternalVersion.WithKind("UpdateOptions"))
	if err != nil {
		return nil, nil, err
	}

	var versionedDeleteOptions runtime.Object //表示特定版本下的 DeleteOptions 实例（如 v1.DeleteOptions）；
	var versionedDeleterObject interface{}    //通过指针获取的实际对象（可能用于后续设置字段）；
	deleteReturnsDeletedObject := false       //表示 delete 操作是否会返回被删掉的完整对象（true/false）。
	//如果当前资源支持“优雅删除”功能（graceful deletion），则构造一个当前 API 版本的 DeleteOptions 对象，并判断该资源是否会返回被删除的完整对象。
	if isGracefulDeleter {
		//这里构造一个当前版本（如 v1）的 DeleteOptions 对象；
		//类似于 New(&v1.DeleteOptions{})，用于客户端传来的删除请求参数的反序列化和校验。
		versionedDeleteOptions, err = a.group.Creater.New(optionsExternalVersion.WithKind("DeleteOptions"))
		if err != nil {
			return nil, nil, err
		}
		versionedDeleterObject = indirectArbitraryPointer(versionedDeleteOptions)
		//如果这个资源实现了 rest.MayReturnFullObjectDeleter 接口，表示它可能在 Delete 操作中返回完整对象；
		//那么就调用接口方法 DeleteReturnsDeletedObject() 来确认是否启用这个行为。
		if mayReturnFullObjectDeleter, ok := storage.(rest.MayReturnFullObjectDeleter); ok {
			deleteReturnsDeletedObject = mayReturnFullObjectDeleter.DeleteReturnsDeletedObject()
		}
	}

	versionedStatusPtr, err := a.group.Creater.New(optionsExternalVersion.WithKind("Status"))
	if err != nil {
		return nil, nil, err
	}
	versionedStatus := indirectArbitraryPointer(versionedStatusPtr)
	var (
		getOptions             runtime.Object          // 内部版 GetOptions 实例
		versionedGetOptions    runtime.Object          // 外部版（versioned）GetOptions 实例
		getOptionsInternalKind schema.GroupVersionKind // 内部版的 GVK 类型
		getSubpath             bool                    // 是否支持 subpath
	)

	//，准备好 Get 请求所需要的 options 对象，包括内部版本和外部版本的类型对象。
	//它是 Kubernetes apiserver 注册资源处理逻辑中的一部分，
	//主要是为了支持客户端通过 /resource/name?param=xx 带参数访问资源时使用自定义的 GetOptions。
	// GetterWithOptions 接口（比普通的 Getter 更强，支持自定义 get 参数，比如 subpath、版本转换等）。
	if isGetterWithOptions {
		//getOptions: 内部版的 GetOptions 对象
		//getSubpath: 是否支持子路径
		getOptions, getSubpath, _ = getterWithOptions.NewGetOptions()
		getOptionsInternalKinds, _, err := a.group.Typer.ObjectKinds(getOptions)
		if err != nil {
			return nil, nil, err
		}
		getOptionsInternalKind = getOptionsInternalKinds[0]
		//尝试用内部 group.GroupVersion 创建外部版本的 GetOptions 对象（会经过 scheme 的版本转换）。
		//如果失败，则退而使用 optionsExternalVersion（外部公共版本）再试一次。
		versionedGetOptions, err = a.group.Creater.New(a.group.GroupVersion.WithKind(getOptionsInternalKind.Kind))
		if err != nil {
			versionedGetOptions, err = a.group.Creater.New(optionsExternalVersion.WithKind(getOptionsInternalKind.Kind))
			if err != nil {
				return nil, nil, err
			}
		}
		isGetter = true
	}

	var versionedWatchEvent interface{}
	if isWatcher {
		versionedWatchEventPtr, err := a.group.Creater.New(a.group.GroupVersion.WithKind("WatchEvent"))
		if err != nil {
			return nil, nil, err
		}
		versionedWatchEvent = indirectArbitraryPointer(versionedWatchEventPtr)
	}

	var (
		connectOptions             runtime.Object
		versionedConnectOptions    runtime.Object
		connectOptionsInternalKind schema.GroupVersionKind
		connectSubpath             bool
	)
	//在资源的 storage 实现了 rest.Connecter 接口的情况下，构建其所需要的 ConnectOptions 参数对象（含内部与版本化对象），
	//用于注册连接型请求（比如 exec、attach、port-forward 这类 WebSocket 或升级连接）。
	if isConnecter {
		connectOptions, connectSubpath, _ = connecter.NewConnectOptions()
		if connectOptions != nil {
			connectOptionsInternalKinds, _, err := a.group.Typer.ObjectKinds(connectOptions)
			if err != nil {
				return nil, nil, err
			}

			connectOptionsInternalKind = connectOptionsInternalKinds[0]
			versionedConnectOptions, err = a.group.Creater.New(a.group.GroupVersion.WithKind(connectOptionsInternalKind.Kind))
			if err != nil {
				versionedConnectOptions, err = a.group.Creater.New(optionsExternalVersion.WithKind(connectOptionsInternalKind.Kind))
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}
	//只有同时支持 List 和 Watch 才允许用 watch=true 监听资源列表。
	allowWatchList := isWatcher && isLister // watching on lists is allowed only for kinds that support both watch and list.
	//这是用于 RESTful API 路径 /namespaces/{ns}/pods/{name} 或 /pods/proxy/{path} 这种形式的参数。
	nameParam := ws.PathParameter("name", "name of the "+kind).DataType("string")
	pathParam := ws.PathParameter("path", "path to the resource").DataType("string")

	params := []*restful.Parameter{}
	actions := []action{}

	var resourceKind string
	//如果 storage 实现了 KindProvider，就用它提供的 kind。
	//否则使用传入的默认 kind。
	//这个字段是最终注册到 OpenAPI 和 discovery 中的资源类型。
	kindProvider, ok := storage.(rest.KindProvider)
	if ok {
		resourceKind = kindProvider.Kind()
	} else {
		resourceKind = kind
	}
	//如果资源支持 List 操作（isLister），必须要实现 TableConvertor。
	//TableConvertor 是用来支持 kubectl get 表格输出的，比如列出 pods 时显示列名/字段。
	tableProvider, isTableProvider := storage.(rest.TableConvertor)
	if isLister && !isTableProvider {
		// All listers must implement TableProvider
		return nil, nil, fmt.Errorf("%q must implement TableConvertor", resource)
	}

	var apiResource metav1.APIResource
	//这部分逻辑是为 API discovery 构建资源元信息：
	//开启了 StorageVersionHash 功能门；
	//并且 storage 提供了存储版本信息；
	//则根据 GVK 生成 StorageVersionHash。
	if utilfeature.DefaultFeatureGate.Enabled(features.StorageVersionHash) &&
		isStorageVersionProvider &&
		storageVersionProvider.StorageVersion() != nil {
		versioner := storageVersionProvider.StorageVersion()
		gvk, err := getStorageVersionKind(versioner, storage, a.group.Typer)
		if err != nil {
			return nil, nil, err
		}
		apiResource.StorageVersionHash = discovery.StorageVersionHash(gvk.Group, gvk.Version, gvk.Kind)
	}

	// Get the list of actions for the given scope.
	//根据资源是否为 命名空间作用域（Namespaced），分别构造 REST 路径；
	//
	//根据资源支持的操作能力（是否能 GET/POST/PATCH/DELETE 等），生成相应的 action；
	//
	//每个 action 代表一个路由项：包含方法（GET/POST）、路径、参数、命名策略等；
	//
	//最终这些 actions 会注册到 HTTP Server（由 go-restful 实现）中。
	switch {
	case !namespaceScoped:
		// Handle non-namespace scoped resources like nodes.
		resourcePath := resource // e.g. "nodes"
		resourceParams := params
		itemPath := resourcePath + "/{name}" // e.g. "nodes/{name}"
		nameParams := append(params, nameParam)
		proxyParams := append(nameParams, pathParam)
		suffix := ""
		//如果是子资源（isSubresource 为 true），则路径追加 /subresource。
		if isSubresource {
			suffix = "/" + subresource
			itemPath = itemPath + suffix
			resourcePath = itemPath
			resourceParams = nameParams
		}
		apiResource.Name = path
		apiResource.Namespaced = false
		apiResource.Kind = resourceKind
		//设置命名规则（不包含 namespace）：
		namer := handlers.ContextBasedNaming{
			Namer:         a.group.Namer,
			ClusterScoped: true,
		}

		// Handler for standard REST verbs (GET, PUT, POST and DELETE).
		// Add actions at the resource path: /api/apiVersion/resource
		//HTTP 方法	路径	含义
		//GET	/nodes	List nodes
		//POST	/nodes	Create node
		//DELETE	/nodes	DeleteCollection
		//GET	/nodes/{name}	Get node
		//PUT	/nodes/{name}	Update node
		//PATCH	/nodes/{name}	Patch node
		//DELETE	/nodes/{name}	Delete node
		//WATCH	/watch/nodes	Watch all nodes
		//WATCH	/watch/nodes/{name}	Watch specific node
		//CONNECT	/nodes/{name}	Connect (e.g., exec/logs)
		//CONNECT	/nodes/{name}/{path:*}	Connect with path
		actions = appendIf(actions, action{"LIST", resourcePath, resourceParams, namer, false}, isLister)
		actions = appendIf(actions, action{"POST", resourcePath, resourceParams, namer, false}, isCreater)
		actions = appendIf(actions, action{"DELETECOLLECTION", resourcePath, resourceParams, namer, false}, isCollectionDeleter)
		// DEPRECATED in 1.11
		actions = appendIf(actions, action{"WATCHLIST", "watch/" + resourcePath, resourceParams, namer, false}, allowWatchList)

		// Add actions at the item path: /api/apiVersion/resource/{name}
		actions = appendIf(actions, action{"GET", itemPath, nameParams, namer, false}, isGetter)
		if getSubpath {
			actions = appendIf(actions, action{"GET", itemPath + "/{path:*}", proxyParams, namer, false}, isGetter)
		}
		actions = appendIf(actions, action{"PUT", itemPath, nameParams, namer, false}, isUpdater)
		actions = appendIf(actions, action{"PATCH", itemPath, nameParams, namer, false}, isPatcher)
		actions = appendIf(actions, action{"DELETE", itemPath, nameParams, namer, false}, isGracefulDeleter)
		// DEPRECATED in 1.11
		actions = appendIf(actions, action{"WATCH", "watch/" + itemPath, nameParams, namer, false}, isWatcher)
		actions = appendIf(actions, action{"CONNECT", itemPath, nameParams, namer, false}, isConnecter)
		actions = appendIf(actions, action{"CONNECT", itemPath + "/{path:*}", proxyParams, namer, false}, isConnecter && connectSubpath)
	default:
		namespaceParamName := "namespaces" // // e.g. namespaces/{namespace}/pods
		// Handler for standard REST verbs (GET, PUT, POST and DELETE).
		namespaceParam := ws.PathParameter("namespace", "object name and auth scope, such as for teams and projects").DataType("string")
		namespacedPath := namespaceParamName + "/{namespace}/" + resource
		namespaceParams := []*restful.Parameter{namespaceParam}

		resourcePath := namespacedPath
		resourceParams := namespaceParams
		itemPath := namespacedPath + "/{name}" //   // e.g. namespaces/{namespace}/pods/{name}
		nameParams := append(namespaceParams, nameParam)
		proxyParams := append(nameParams, pathParam)
		itemPathSuffix := ""
		if isSubresource {
			itemPathSuffix = "/" + subresource
			itemPath = itemPath + itemPathSuffix
			resourcePath = itemPath
			resourceParams = nameParams
		}
		apiResource.Name = path
		apiResource.Namespaced = true
		apiResource.Kind = resourceKind
		namer := handlers.ContextBasedNaming{
			Namer:         a.group.Namer,
			ClusterScoped: false,
		}

		actions = appendIf(actions, action{"LIST", resourcePath, resourceParams, namer, false}, isLister)
		actions = appendIf(actions, action{"POST", resourcePath, resourceParams, namer, false}, isCreater)
		actions = appendIf(actions, action{"DELETECOLLECTION", resourcePath, resourceParams, namer, false}, isCollectionDeleter)
		// DEPRECATED in 1.11
		actions = appendIf(actions, action{"WATCHLIST", "watch/" + resourcePath, resourceParams, namer, false}, allowWatchList)

		actions = appendIf(actions, action{"GET", itemPath, nameParams, namer, false}, isGetter)
		if getSubpath {
			actions = appendIf(actions, action{"GET", itemPath + "/{path:*}", proxyParams, namer, false}, isGetter)
		}
		actions = appendIf(actions, action{"PUT", itemPath, nameParams, namer, false}, isUpdater)
		actions = appendIf(actions, action{"PATCH", itemPath, nameParams, namer, false}, isPatcher)
		actions = appendIf(actions, action{"DELETE", itemPath, nameParams, namer, false}, isGracefulDeleter)
		// DEPRECATED in 1.11
		actions = appendIf(actions, action{"WATCH", "watch/" + itemPath, nameParams, namer, false}, isWatcher)
		actions = appendIf(actions, action{"CONNECT", itemPath, nameParams, namer, false}, isConnecter)
		actions = appendIf(actions, action{"CONNECT", itemPath + "/{path:*}", proxyParams, namer, false}, isConnecter && connectSubpath)

		// list or post across namespace.
		// For ex: LIST all pods in all namespaces by sending a LIST request at /api/apiVersion/pods.
		// TODO: more strongly type whether a resource allows these actions on "all namespaces" (bulk delete)
		//额外注册跨命名空间的 LIST 和 WATCHLIST：
		if !isSubresource {
			actions = appendIf(actions, action{"LIST", resource, params, namer, true}, isLister)
			// DEPRECATED in 1.11
			actions = appendIf(actions, action{"WATCHLIST", "watch/" + resource, params, namer, true}, allowWatchList)
		}
	}

	var resourceInfo *storageversion.ResourceInfo
	//features.StorageVersionAPI 和 features.APIServerIdentity：这两个特性开关控制是否启用了与存储版本和 API 服务器身份相关的功能。
	if utilfeature.DefaultFeatureGate.Enabled(features.StorageVersionAPI) &&
		utilfeature.DefaultFeatureGate.Enabled(features.APIServerIdentity) &&
		isStorageVersionProvider &&
		storageVersionProvider.StorageVersion() != nil {

		versioner := storageVersionProvider.StorageVersion()
		encodingGVK, err := getStorageVersionKind(versioner, storage, a.group.Typer)
		if err != nil {
			return nil, nil, err
		}
		decodableVersions := []schema.GroupVersion{}
		if a.group.ConvertabilityChecker != nil {
			decodableVersions = a.group.ConvertabilityChecker.VersionsForGroupKind(fqKindToRegister.GroupKind())
		}

		resourceInfo = &storageversion.ResourceInfo{
			GroupResource: schema.GroupResource{
				Group:    a.group.GroupVersion.Group,
				Resource: apiResource.Name,
			},
			EncodingVersion: encodingGVK.GroupVersion().String(),
			// We record EquivalentResourceMapper first instead of calculate
			// DecodableVersions immediately because API installation must
			// be completed first for us to know equivalent APIs
			EquivalentResourceMapper: a.group.EquivalentResourceRegistry,

			DirectlyDecodableVersions: decodableVersions,

			ServedVersions: a.group.AllServedVersionsByResource[path],
		}
	}

	// Create Routes for the actions.
	// 为动作创建路由。
	// TODO: Add status documentation using Returns()
	// 待办：使用 Returns() 添加状态码文档
	// Errors (see api/errors/errors.go as well as go-restful router):
	// 可能的错误（参考 api/errors/errors.go 和 go-restful 路由器的实现）：
	// http.StatusNotFound, http.StatusMethodNotAllowed,
	// HTTP 404（未找到）、405（方法不允许）、
	// http.StatusUnsupportedMediaType, http.StatusNotAcceptable,
	// 415（不支持的媒体类型）、406（不可接受）、
	// http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden,
	// 400（错误请求）、401（未授权）、403（禁止访问）、
	// http.StatusRequestTimeout, http.StatusConflict, http.StatusPreconditionFailed,
	// 408（请求超时）、409（冲突）、412（前置条件失败）、
	// http.StatusUnprocessableEntity, http.StatusInternalServerError,
	// 422（无法处理的实体）、500（服务器内部错误）、
	// http.StatusServiceUnavailable
	// 503（服务不可用）
	// and api error codes
	// 以及API自定义错误码
	// Note that if we specify a versioned Status object here, we may need to
	// 注意：如果在此指定了版本化的 Status 对象，
	// create one for the tests, also
	// 可能还需要为测试创建一个对应的版本
	// Success:
	// 成功状态码：
	// http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent
	// 200（成功）、201（已创建）、202（已接受）、204（无内容）
	//
	// test/integration/auth_test.go is currently the most comprehensive status code test
	// test/integration/auth_test.go 是目前最全面的状态码测试用例
	for _, s := range a.group.Serializer.SupportedMediaTypes() {
		if len(s.MediaTypeSubType) == 0 || len(s.MediaTypeType) == 0 {
			return nil, nil, fmt.Errorf("all serializers in the group Serializer must have MediaTypeType and MediaTypeSubType set: %s", s.MediaType)
		}
	}
	mediaTypes, streamMediaTypes := negotiation.MediaTypesForSerializer(a.group.Serializer)
	allMediaTypes := append(mediaTypes, streamMediaTypes...)
	ws.Produces(allMediaTypes...)

	kubeVerbs := map[string]struct{}{}
	reqScope := handlers.RequestScope{
		Serializer:      a.group.Serializer,
		ParameterCodec:  a.group.ParameterCodec,
		Creater:         a.group.Creater,
		Convertor:       a.group.Convertor,
		Defaulter:       a.group.Defaulter,
		Typer:           a.group.Typer,
		UnsafeConvertor: a.group.UnsafeConvertor,
		Authorizer:      a.group.Authorizer,

		EquivalentResourceMapper: a.group.EquivalentResourceRegistry,

		// TODO: Check for the interface on storage
		TableConvertor: tableProvider,

		// TODO: This seems wrong for cross-group subresources. It makes an assumption that a subresource and its parent are in the same group version. Revisit this.
		Resource:    a.group.GroupVersion.WithResource(resource),
		Subresource: subresource,
		Kind:        fqKindToRegister,

		AcceptsGroupVersionDelegate: gvAcceptor,

		HubGroupVersion: schema.GroupVersion{Group: fqKindToRegister.Group, Version: runtime.APIVersionInternal},

		MetaGroupVersion: metav1.SchemeGroupVersion,

		MaxRequestBodyBytes: a.group.MaxRequestBodyBytes,
	}
	if a.group.MetaGroupVersion != nil {
		reqScope.MetaGroupVersion = *a.group.MetaGroupVersion
	}

	// Strategies may ignore changes to some fields by resetting the field values.
	//
	// For instance, spec resource strategies should reset the status, and status subresource
	// strategies should reset the spec.
	//
	// Strategies that reset fields must report to the field manager which fields are
	// reset by implementing either the ResetFieldsStrategy or the ResetFieldsFilterStrategy
	// interface.
	//
	// For subresources that provide write access to only specific nested fields
	// fieldpath.NewPatternFilter can help create a filter to reset all other fields.
	//背景：
	//Kubernetes 有不同的策略（strategies）来处理资源更新，比如 spec 策略和 status 策略。
	//有时需要忽略某些字段的变化（例如 spec 策略应该重置 status 字段，status 策略应该重置 spec 字段）。
	//核心逻辑：
	//代码检查存储后端（storage）是否实现了两种可能的接口：
	//ResetFieldsStrategy：通过 GetResetFields() 返回需要排除的字段
	//ResetFieldsFilterStrategy：通过 GetResetFieldsFilter() 返回一个过滤器
	//这两种接口是互斥的，不能同时实现。
	//字段过滤器：
	//根据实现的接口不同，创建不同的字段过滤器（resetFieldsFilter）：
	//对于 ResetFieldsStrategy，使用 fieldpath.NewExcludeFilterSetMap 创建排除指定字段的过滤器
	//对于 ResetFieldsFilterStrategy，直接使用它提供的过滤器
	//字段管理器创建：
	//最后用这些配置创建默认的字段管理器（DefaultFieldManager），它会负责：
	//跟踪字段的变化
	//应用字段过滤器
	//处理字段的合并和冲突
	//目的：
	//这段代码的主要目的是确保在更新资源时，某些字段（如 spec 或 status）能够被正确地忽略或重置，同时将这些重置操作正确地反映到字段管理器中。
	var resetFieldsFilter map[fieldpath.APIVersion]fieldpath.Filter
	resetFieldsStrategy, isResetFieldsStrategy := storage.(rest.ResetFieldsStrategy)
	if isResetFieldsStrategy {
		resetFieldsFilter = fieldpath.NewExcludeFilterSetMap(resetFieldsStrategy.GetResetFields())
	}
	if resetFieldsStrategy, isResetFieldsFilterStrategy := storage.(rest.ResetFieldsFilterStrategy); isResetFieldsFilterStrategy {
		if isResetFieldsStrategy {
			return nil, nil, fmt.Errorf("may not implement both ResetFieldsStrategy and ResetFieldsFilterStrategy")
		}
		resetFieldsFilter = resetFieldsStrategy.GetResetFieldsFilter()
	}

	reqScope.FieldManager, err = managedfields.NewDefaultFieldManager(
		a.group.TypeConverter,
		a.group.UnsafeConvertor,
		a.group.Defaulter,
		a.group.Creater,
		fqKindToRegister,
		reqScope.HubGroupVersion,
		subresource,
		resetFieldsFilter,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create field manager: %v", err)
	}

	for _, action := range actions {
		producedObject := storageMeta.ProducesObject(action.Verb)
		if producedObject == nil {
			producedObject = defaultVersionedObject
		}
		reqScope.Namer = action.Namer

		requestScope := "cluster"
		//namespaced 用于标识资源是否是 Namespaced 的，
		//operationSuffix 是操作名称的后缀，用于构建最终的操作名称（比如 getNamespacedPodWithPath 这类名字）。
		var namespaced string
		var operationSuffix string
		if apiResource.Namespaced {
			requestScope = "namespace"
			namespaced = "Namespaced"
		}
		//如果路径以 /{path:*} 结尾，说明是带子路径的请求（比如 proxy 或 connect 子资源），
		//将作用域设为 "resource"，并添加 "WithPath" 到操作后缀。
		if strings.HasSuffix(action.Path, "/{path:*}") {
			requestScope = "resource"
			operationSuffix = operationSuffix + "WithPath"
		}
		//如果路径中包含 /{name}（即操作的是某个特定资源）或是 POST 请求（创建资源），则作用域为 "resource"，意味着是对某个具体资源操作。
		if strings.Contains(action.Path, "/{name}") || action.Verb == "POST" {
			requestScope = "resource"
		}
		//如果是跨命名空间的操作（例如：GET /api/v1/pods 查询所有命名空间下的 pod），
		//那是集群级别的操作，作用域设置为 "cluster"，操作名加后缀 "ForAllNamespaces"，并清空 namespaced。
		if action.AllNamespaces {
			requestScope = "cluster"
			operationSuffix = operationSuffix + "ForAllNamespaces"
			namespaced = ""
		}
		//用于收集这个资源支持的所有动词。
		if kubeVerb, found := toDiscoveryKubeVerb[action.Verb]; found {
			if len(kubeVerb) != 0 {
				kubeVerbs[kubeVerb] = struct{}{}
			}
		} else {
			return nil, nil, fmt.Errorf("unknown action verb for discovery: %s", action.Verb)
		}

		routes := []*restful.RouteBuilder{}

		// If there is a subresource, kind should be the parent's kind.
		if isSubresource {
			//如果当前注册的是一个 子资源（如 /pods/status），
			//就需要获取它对应的父资源（如 pods）的 storage 和 Kind（因为子资源的 GVK 通常是继承父资源的 Kind）。
			parentStorage, ok := a.group.Storage[resource]
			if !ok {
				return nil, nil, fmt.Errorf("missing parent storage: %q", resource)
			}

			fqParentKind, err := GetResourceKind(a.group.GroupVersion, parentStorage, a.group.Typer)
			if err != nil {
				return nil, nil, err
			}
			kind = fqParentKind.Kind
		}
		//尝试判断当前 storage 是否实现了 StorageMetricsOverride 接口。如果实现了，
		//这意味着这个资源对 HTTP Verb 的监控指标行为做了特殊处理。
		verbOverrider, needOverride := storage.(StorageMetricsOverride)

		// accumulate endpoint-level warnings
		//初始化一些变量，用来记录当前 API 是否弃用、何时移除、以及要给用户展示的 warning。
		var (
			warnings       []string
			deprecated     bool
			removedRelease string
		)

		{
			//拿一个资源对象的副本并设置它的 GVK；
			//获取当前 Kubernetes 主版本号和次版本号；
			//判断这个资源是否在当前版本下已被标记为弃用；
			//如果弃用，记录它在哪个版本将被移除，以及对应的 warning message（比如 "This version will be removed in v1.30"）。
			versionedPtrWithGVK := versionedPtr.DeepCopyObject()
			versionedPtrWithGVK.GetObjectKind().SetGroupVersionKind(fqKindToRegister)
			currentMajor, currentMinor, _ := deprecation.MajorMinor(versioninfo.Get())
			deprecated = deprecation.IsDeprecated(versionedPtrWithGVK, currentMajor, currentMinor)
			if deprecated {
				removedRelease = deprecation.RemovedRelease(versionedPtrWithGVK)
				warnings = append(warnings, deprecation.WarningMessage(versionedPtrWithGVK))
			}
		}

		switch action.Verb {
		case "GET": // Get a resource.
			var handler restful.RouteFunction
			if isGetterWithOptions {
				//如果 storage 实现了 GetterWithOptions（支持像 GET /pod?export=true 这样的选项），就调用 restfulGetResourceWithOptions。
				handler = restfulGetResourceWithOptions(getterWithOptions, reqScope, isSubresource)
			} else {
				handler = restfulGetResource(getter, reqScope)
			}

			if needOverride {
				// need change the reported verb
				handler = metrics.InstrumentRouteFunc(verbOverrider.OverrideMetricsVerb(action.Verb), group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, handler)
			} else {
				handler = metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, handler)
			}
			handler = utilwarning.AddWarningsHandler(handler, warnings)

			doc := "read the specified " + kind
			if isSubresource {
				doc = "read " + subresource + " of the specified " + kind
			}
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("read"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				Writes(producedObject)
			if isGetterWithOptions {
				if err := AddObjectParams(ws, route, versionedGetOptions); err != nil {
					return nil, nil, err
				}
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case "LIST": // List all resources of a kind.
			doc := "list objects of kind " + kind
			if isSubresource {
				doc = "list " + subresource + " of objects of kind " + kind
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulListResource(lister, watcher, reqScope, false, a.minRequestTimeout))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("list"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), allMediaTypes...)...).
				Returns(http.StatusOK, "OK", versionedList).
				Writes(versionedList)
			if err := AddObjectParams(ws, route, versionedListOptions); err != nil {
				return nil, nil, err
			}
			switch {
			case isLister && isWatcher:
				doc := "list or watch objects of kind " + kind
				if isSubresource {
					doc = "list or watch " + subresource + " of objects of kind " + kind
				}
				route.Doc(doc)
			case isWatcher:
				doc := "watch objects of kind " + kind
				if isSubresource {
					doc = "watch " + subresource + "of objects of kind " + kind
				}
				route.Doc(doc)
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case "PUT": // Update a resource.
			doc := "replace the specified " + kind
			if isSubresource {
				doc = "replace " + subresource + " of the specified " + kind
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulUpdateResource(updater, reqScope, admit))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.PUT(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("replace"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				// TODO: in some cases, the API may return a v1.Status instead of the versioned object
				// but currently go-restful can't handle multiple different objects being returned.
				Returns(http.StatusCreated, "Created", producedObject).
				Reads(defaultVersionedObject).
				Writes(producedObject)
			if err := AddObjectParams(ws, route, versionedUpdateOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case "PATCH": // Partially update a resource
			doc := "partially update the specified " + kind
			if isSubresource {
				doc = "partially update " + subresource + " of the specified " + kind
			}
			supportedTypes := []string{
				string(types.JSONPatchType),
				string(types.MergePatchType),
				string(types.StrategicMergePatchType),
				string(types.ApplyYAMLPatchType),
			}
			if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
				supportedTypes = append(supportedTypes, string(types.ApplyCBORPatchType))
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulPatchResource(patcher, reqScope, admit, supportedTypes))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.PATCH(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Consumes(supportedTypes...).
				Operation("patch"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				// Patch can return 201 when a server side apply is requested
				Returns(http.StatusCreated, "Created", producedObject).
				Reads(metav1.Patch{}).
				Writes(producedObject)
			if err := AddObjectParams(ws, route, versionedPatchOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case "POST": // Create a resource.
			var handler restful.RouteFunction
			if isNamedCreater {
				handler = restfulCreateNamedResource(namedCreater, reqScope, admit)
			} else {
				handler = restfulCreateResource(creater, reqScope, admit)
			}
			handler = metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, handler)
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			article := GetArticleForNoun(kind, " ")
			doc := "create" + article + kind
			if isSubresource {
				doc = "create " + subresource + " of" + article + kind
			}
			route := ws.POST(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("create"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Returns(http.StatusOK, "OK", producedObject).
				// TODO: in some cases, the API may return a v1.Status instead of the versioned object
				// but currently go-restful can't handle multiple different objects being returned.
				Returns(http.StatusCreated, "Created", producedObject).
				Returns(http.StatusAccepted, "Accepted", producedObject).
				Reads(defaultVersionedObject).
				Writes(producedObject)
			if err := AddObjectParams(ws, route, versionedCreateOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case "DELETE": // Delete a resource.
			article := GetArticleForNoun(kind, " ")
			doc := "delete" + article + kind
			if isSubresource {
				doc = "delete " + subresource + " of" + article + kind
			}
			deleteReturnType := versionedStatus
			if deleteReturnsDeletedObject {
				deleteReturnType = producedObject
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulDeleteResource(gracefulDeleter, isGracefulDeleter, reqScope, admit))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.DELETE(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("delete"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Writes(deleteReturnType).
				Returns(http.StatusOK, "OK", deleteReturnType).
				Returns(http.StatusAccepted, "Accepted", deleteReturnType)
			if isGracefulDeleter {
				route.Reads(versionedDeleterObject)
				route.ParameterNamed("body").Required(false)
				if err := AddObjectParams(ws, route, versionedDeleteOptions); err != nil {
					return nil, nil, err
				}
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case "DELETECOLLECTION":
			doc := "delete collection of " + kind
			if isSubresource {
				doc = "delete collection of " + subresource + " of a " + kind
			}
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulDeleteCollection(collectionDeleter, isCollectionDeleter, reqScope, admit))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.DELETE(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("deletecollection"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(append(storageMeta.ProducesMIMETypes(action.Verb), mediaTypes...)...).
				Writes(versionedStatus).
				Returns(http.StatusOK, "OK", versionedStatus)
			if isCollectionDeleter {
				route.Reads(versionedDeleterObject)
				route.ParameterNamed("body").Required(false)
				if err := AddObjectParams(ws, route, versionedDeleteOptions); err != nil {
					return nil, nil, err
				}
			}
			if err := AddObjectParams(ws, route, versionedListOptions, "watch", "allowWatchBookmarks"); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		// deprecated in 1.11
		case "WATCH": // Watch a resource.
			doc := "watch changes to an object of kind " + kind
			if isSubresource {
				doc = "watch changes to " + subresource + " of an object of kind " + kind
			}
			doc += ". deprecated: use the 'watch' parameter with a list operation instead, filtered to a single item with the 'fieldSelector' parameter."
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulListResource(lister, watcher, reqScope, true, a.minRequestTimeout))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("watch"+namespaced+kind+strings.Title(subresource)+operationSuffix).
				Produces(allMediaTypes...).
				Returns(http.StatusOK, "OK", versionedWatchEvent).
				Writes(versionedWatchEvent)
			if err := AddObjectParams(ws, route, versionedListOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		// deprecated in 1.11
		case "WATCHLIST": // Watch all resources of a kind.
			doc := "watch individual changes to a list of " + kind
			if isSubresource {
				doc = "watch individual changes to a list of " + subresource + " of " + kind
			}
			doc += ". deprecated: use the 'watch' parameter with a list operation instead."
			handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulListResource(lister, watcher, reqScope, true, a.minRequestTimeout))
			handler = utilwarning.AddWarningsHandler(handler, warnings)
			route := ws.GET(action.Path).To(handler).
				Doc(doc).
				Param(ws.QueryParameter("pretty", "If 'true', then the output is pretty printed. Defaults to 'false' unless the user-agent indicates a browser or command-line HTTP tool (curl and wget).")).
				Operation("watch"+namespaced+kind+strings.Title(subresource)+"List"+operationSuffix).
				Produces(allMediaTypes...).
				Returns(http.StatusOK, "OK", versionedWatchEvent).
				Writes(versionedWatchEvent)
			if err := AddObjectParams(ws, route, versionedListOptions); err != nil {
				return nil, nil, err
			}
			addParams(route, action.Params)
			routes = append(routes, route)
		case "CONNECT":
			for _, method := range connecter.ConnectMethods() {
				connectProducedObject := storageMeta.ProducesObject(method)
				if connectProducedObject == nil {
					connectProducedObject = "string"
				}
				doc := "connect " + method + " requests to " + kind
				if isSubresource {
					doc = "connect " + method + " requests to " + subresource + " of " + kind
				}
				handler := metrics.InstrumentRouteFunc(action.Verb, group, version, resource, subresource, requestScope, metrics.APIServerComponent, deprecated, removedRelease, restfulConnectResource(connecter, reqScope, admit, path, isSubresource))
				handler = utilwarning.AddWarningsHandler(handler, warnings)
				route := ws.Method(method).Path(action.Path).
					To(handler).
					Doc(doc).
					Operation("connect" + strings.Title(strings.ToLower(method)) + namespaced + kind + strings.Title(subresource) + operationSuffix).
					Produces("*/*").
					Consumes("*/*").
					Writes(connectProducedObject)
				if versionedConnectOptions != nil {
					if err := AddObjectParams(ws, route, versionedConnectOptions); err != nil {
						return nil, nil, err
					}
				}
				addParams(route, action.Params)
				routes = append(routes, route)

				// transform ConnectMethods to kube verbs
				if kubeVerb, found := toDiscoveryKubeVerb[method]; found {
					if len(kubeVerb) != 0 {
						kubeVerbs[kubeVerb] = struct{}{}
					}
				}
			}
		default:
			return nil, nil, fmt.Errorf("unrecognized action verb: %s", action.Verb)
		}
		for _, route := range routes {
			route.Metadata(RouteMetaGVK, metav1.GroupVersionKind{
				Group:   reqScope.Kind.Group,
				Version: reqScope.Kind.Version,
				Kind:    reqScope.Kind.Kind,
			})
			route.Metadata(RouteMetaAction, strings.ToLower(action.Verb))
			ws.Route(route)
		}
		// Note: update GetAuthorizerAttributes() when adding a custom handler.
	}

	apiResource.Verbs = make([]string, 0, len(kubeVerbs))
	for kubeVerb := range kubeVerbs {
		apiResource.Verbs = append(apiResource.Verbs, kubeVerb)
	}
	sort.Strings(apiResource.Verbs)

	if shortNamesProvider, ok := storage.(rest.ShortNamesProvider); ok {
		apiResource.ShortNames = shortNamesProvider.ShortNames()
	}
	if categoriesProvider, ok := storage.(rest.CategoriesProvider); ok {
		apiResource.Categories = categoriesProvider.Categories()
	}
	if !isSubresource {
		singularNameProvider, ok := storage.(rest.SingularNameProvider)
		if !ok {
			return nil, nil, fmt.Errorf("resource %s must implement SingularNameProvider", resource)
		}
		apiResource.SingularName = singularNameProvider.GetSingularName()
	}

	if gvkProvider, ok := storage.(rest.GroupVersionKindProvider); ok {
		gvk := gvkProvider.GroupVersionKind(a.group.GroupVersion)
		apiResource.Group = gvk.Group
		apiResource.Version = gvk.Version
		apiResource.Kind = gvk.Kind
	}

	// Record the existence of the GVR and the corresponding GVK
	a.group.EquivalentResourceRegistry.RegisterKindFor(reqScope.Resource, reqScope.Subresource, fqKindToRegister)

	return &apiResource, resourceInfo, nil
}

// indirectArbitraryPointer returns *ptrToObject for an arbitrary pointer
func indirectArbitraryPointer(ptrToObject interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(ptrToObject)).Interface()
}

func appendIf(actions []action, a action, shouldAppend bool) []action {
	if shouldAppend {
		actions = append(actions, a)
	}
	return actions
}

func addParams(route *restful.RouteBuilder, params []*restful.Parameter) {
	for _, param := range params {
		route.Param(param)
	}
}

// AddObjectParams converts a runtime.Object into a set of go-restful Param() definitions on the route.
// The object must be a pointer to a struct; only fields at the top level of the struct that are not
// themselves interfaces or structs are used; only fields with a json tag that is non empty (the standard
// Go JSON behavior for omitting a field) become query parameters. The name of the query parameter is
// the JSON field name. If a description struct tag is set on the field, that description is used on the
// query parameter. In essence, it converts a standard JSON top level object into a query param schema.
func AddObjectParams(ws *restful.WebService, route *restful.RouteBuilder, obj interface{}, excludedNames ...string) error {
	sv, err := conversion.EnforcePtr(obj)
	if err != nil {
		return err
	}
	st := sv.Type()
	excludedNameSet := sets.NewString(excludedNames...)
	switch st.Kind() {
	case reflect.Struct:
		for i := 0; i < st.NumField(); i++ {
			name := st.Field(i).Name
			sf, ok := st.FieldByName(name)
			if !ok {
				continue
			}
			switch sf.Type.Kind() {
			case reflect.Interface, reflect.Struct:
			case reflect.Pointer:
				// TODO: This is a hack to let metav1.Time through. This needs to be fixed in a more generic way eventually. bug #36191
				if (sf.Type.Elem().Kind() == reflect.Interface || sf.Type.Elem().Kind() == reflect.Struct) && strings.TrimPrefix(sf.Type.String(), "*") != "metav1.Time" {
					continue
				}
				fallthrough
			default:
				jsonTag := sf.Tag.Get("json")
				if len(jsonTag) == 0 {
					continue
				}
				jsonName := strings.SplitN(jsonTag, ",", 2)[0]
				if len(jsonName) == 0 {
					continue
				}
				if excludedNameSet.Has(jsonName) {
					continue
				}
				var desc string
				if docable, ok := obj.(documentable); ok {
					desc = docable.SwaggerDoc()[jsonName]
				}
				route.Param(ws.QueryParameter(jsonName, desc).DataType(typeToJSON(sf.Type.String())))
			}
		}
	}
	return nil
}

// TODO: this is incomplete, expand as needed.
// Convert the name of a golang type to the name of a JSON type
func typeToJSON(typeName string) string {
	switch typeName {
	case "bool", "*bool":
		return "boolean"
	case "uint8", "*uint8", "int", "*int", "int32", "*int32", "int64", "*int64", "uint32", "*uint32", "uint64", "*uint64":
		return "integer"
	case "float64", "*float64", "float32", "*float32":
		return "number"
	case "metav1.Time", "*metav1.Time":
		return "string"
	case "byte", "*byte":
		return "string"
	case "v1.DeletionPropagation", "*v1.DeletionPropagation":
		return "string"
	case "v1.ResourceVersionMatch", "*v1.ResourceVersionMatch":
		return "string"
	case "v1.IncludeObjectPolicy", "*v1.IncludeObjectPolicy":
		return "string"
	case "*string":
		return "string"

	// TODO: Fix these when go-restful supports a way to specify an array query param:
	// https://github.com/emicklei/go-restful/issues/225
	case "[]string", "[]*string":
		return "string"
	case "[]int32", "[]*int32":
		return "integer"

	default:
		return typeName
	}
}

// defaultStorageMetadata provides default answers to rest.StorageMetadata.
type defaultStorageMetadata struct{}

// defaultStorageMetadata implements rest.StorageMetadata
var _ rest.StorageMetadata = defaultStorageMetadata{}

func (defaultStorageMetadata) ProducesMIMETypes(verb string) []string {
	return nil
}

func (defaultStorageMetadata) ProducesObject(verb string) interface{} {
	return nil
}

// splitSubresource checks if the given storage path is the path of a subresource and returns
// the resource and subresource components.
func splitSubresource(path string) (string, string, error) {
	var resource, subresource string
	switch parts := strings.Split(path, "/"); len(parts) {
	case 2:
		resource, subresource = parts[0], parts[1]
	case 1:
		resource = parts[0]
	default:
		// TODO: support deeper paths
		return "", "", fmt.Errorf("api_installer allows only one or two segment paths (resource or resource/subresource)")
	}
	return resource, subresource, nil
}

// GetArticleForNoun returns the article needed for the given noun.
func GetArticleForNoun(noun string, padding string) string {
	if !strings.HasSuffix(noun, "ss") && strings.HasSuffix(noun, "s") {
		// Plurals don't have an article.
		// Don't catch words like class
		return fmt.Sprintf("%v", padding)
	}

	article := "a"
	if isVowel(rune(noun[0])) {
		article = "an"
	}

	return fmt.Sprintf("%s%s%s", padding, article, padding)
}

// isVowel returns true if the rune is a vowel (case insensitive).
func isVowel(c rune) bool {
	vowels := []rune{'a', 'e', 'i', 'o', 'u'}
	for _, value := range vowels {
		if value == unicode.ToLower(c) {
			return true
		}
	}
	return false
}

func restfulListResource(r rest.Lister, rw rest.Watcher, scope handlers.RequestScope, forceWatch bool, minRequestTimeout time.Duration) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.ListResource(r, rw, &scope, forceWatch, minRequestTimeout)(res.ResponseWriter, req.Request)
	}
}

func restfulCreateNamedResource(r rest.NamedCreater, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.CreateNamedResource(r, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulCreateResource(r rest.Creater, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.CreateResource(r, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulDeleteResource(r rest.GracefulDeleter, allowsOptions bool, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.DeleteResource(r, allowsOptions, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulDeleteCollection(r rest.CollectionDeleter, checkBody bool, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.DeleteCollection(r, checkBody, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulUpdateResource(r rest.Updater, scope handlers.RequestScope, admit admission.Interface) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.UpdateResource(r, &scope, admit)(res.ResponseWriter, req.Request)
	}
}

func restfulPatchResource(r rest.Patcher, scope handlers.RequestScope, admit admission.Interface, supportedTypes []string) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.PatchResource(r, &scope, admit, supportedTypes)(res.ResponseWriter, req.Request)
	}
}

func restfulGetResource(r rest.Getter, scope handlers.RequestScope) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.GetResource(r, &scope)(res.ResponseWriter, req.Request)
	}
}

func restfulGetResourceWithOptions(r rest.GetterWithOptions, scope handlers.RequestScope, isSubresource bool) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.GetResourceWithOptions(r, &scope, isSubresource)(res.ResponseWriter, req.Request)
	}
}

func restfulConnectResource(connecter rest.Connecter, scope handlers.RequestScope, admit admission.Interface, restPath string, isSubresource bool) restful.RouteFunction {
	return func(req *restful.Request, res *restful.Response) {
		handlers.ConnectResource(connecter, &scope, admit, restPath, isSubresource)(res.ResponseWriter, req.Request)
	}
}
