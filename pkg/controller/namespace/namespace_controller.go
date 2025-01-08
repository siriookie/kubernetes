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

package namespace

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/namespace/deletion"

	"k8s.io/klog/v2"
)

const (
	// namespaceDeletionGracePeriod is the time period to wait before processing a received namespace event.
	// This allows time for the following to occur:
	// * lifecycle admission plugins on HA apiservers to also observe a namespace
	//   deletion and prevent new objects from being created in the terminating namespace
	// * non-leader etcd servers to observe last-minute object creations in a namespace
	//   so this controller's cleanup can actually clean up all objects
	//在接收到一个命名空间（namespace）的删除事件后，NamespaceController 等待一定时间再开始处理这个事件的延迟时间，具体来说是 5 秒钟。
	//生命周期（Lifecycle）准入插件的观察时间
	//
	//Kubernetes 有生命周期准入插件（Lifecycle Admission Plugins），它们可以拦截和管理资源的创建和删除请求。
	//在高可用（HA）的 API Server 部署中，不同的 API Server 节点需要观察到同一个命名空间的删除事件。
	//延迟的作用：
	//确保所有 API Server 节点都能够感知到命名空间的删除事件，并禁止在正在删除（Terminating）状态的命名空间中创建新对象。
	//非主节点（Non-leader）etcd 观察时间
	//
	//Kubernetes 使用 etcd 作为存储后端。当命名空间中的对象在删除前被大量快速创建，可能有些操作还没被所有 etcd 节点完全观察到。
	//延迟的作用：
	//给 etcd 集群的非主节点（如 follower 节点）足够的时间去同步和观察命名空间中的新创建对象。
	//确保在后续的清理过程中，NamespaceController 能够删除命名空间内的所有资源对象，而不会漏掉刚刚同步的对象。
	namespaceDeletionGracePeriod = 5 * time.Second
)

// NamespaceController is responsible for performing actions dependent upon a namespace phase
type NamespaceController struct {
	// lister that can list namespaces from a shared cache
	lister corelisters.NamespaceLister
	// returns true when the namespace cache is ready
	listerSynced cache.InformerSynced
	// namespaces that have been queued up for processing by workers
	queue workqueue.TypedRateLimitingInterface[string]
	// helper to delete all resources in the namespace when the namespace is deleted.
	namespacedResourcesDeleter deletion.NamespacedResourcesDeleterInterface
}

// NewNamespaceController creates a new NamespaceController
func NewNamespaceController(
	ctx context.Context,
	kubeClient clientset.Interface,
	metadataClient metadata.Interface,
	discoverResourcesFn func() ([]*metav1.APIResourceList, error),
	namespaceInformer coreinformers.NamespaceInformer,
	resyncPeriod time.Duration,
	finalizerToken v1.FinalizerName) *NamespaceController {

	// create the controller so we can inject the enqueue function
	namespaceController := &NamespaceController{
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			nsControllerRateLimiter(),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "namespace",
			},
		),
		namespacedResourcesDeleter: deletion.NewNamespacedResourcesDeleter(ctx, kubeClient.CoreV1().Namespaces(), metadataClient, kubeClient.CoreV1(), discoverResourcesFn, finalizerToken),
	}

	// configure the namespace informer event handlers
	namespaceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				namespace := obj.(*v1.Namespace)
				namespaceController.enqueueNamespace(namespace)
			},
			//当命名空间从 Active 状态转为 Terminating 状态时会触发 UpdateFunc，此时正是执行资源清理的开始。
			UpdateFunc: func(oldObj, newObj interface{}) {
				namespace := newObj.(*v1.Namespace)
				namespaceController.enqueueNamespace(namespace)
			},
		},
		resyncPeriod,
	)
	namespaceController.lister = namespaceInformer.Lister()
	namespaceController.listerSynced = namespaceInformer.Informer().HasSynced

	return namespaceController
}

// nsControllerRateLimiter is tuned for a faster than normal recycle time with default backoff speed and default overall
// requeing speed.  We do this so that namespace cleanup is reliably faster and we know that the number of namespaces being
// deleted is smaller than total number of other namespace scoped resources in a cluster.
func nsControllerRateLimiter() workqueue.TypedRateLimiter[string] {
	return workqueue.NewTypedMaxOfRateLimiter(
		// this ensures that we retry namespace deletion at least every minute, never longer.
		//起始重试间隔：5ms，确保短时间内能快速重试。
		//最大重试间隔：60秒，失败项最多每分钟会被重试一次。
		workqueue.NewTypedItemExponentialFailureRateLimiter[string](5*time.Millisecond, 60*time.Second),
		// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		//原理：基于一个令牌桶（Token Bucket）算法，限制总体的速率。
		//每秒令牌生成速率：10 QPS，即每秒最多可以处理 10 个命名空间清理操作。
		//桶的大小：100，在突发情况下最多可以排队处理 100 个任务。
		&workqueue.TypedBucketRateLimiter[string]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}

// enqueueNamespace adds an object to the controller work queue
// obj could be an *v1.Namespace, or a DeletionFinalStateUnknown item.
func (nm *NamespaceController) enqueueNamespace(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}

	namespace := obj.(*v1.Namespace)
	// don't queue if we aren't deleted
	//不是待删除的就不要入队了
	if namespace.DeletionTimestamp == nil || namespace.DeletionTimestamp.IsZero() {
		return
	}

	// delay processing namespace events to allow HA api servers to observe namespace deletion,
	// and HA etcd servers to observe last minute object creations inside the namespace
	nm.queue.AddAfter(key, namespaceDeletionGracePeriod)
}

// worker processes the queue of namespace objects.
// Each namespace can be in the queue at most once.
// The system ensures that no two workers can process
// the same namespace at the same time.
// worker 处理命名空间对象的队列。
// 每个命名空间在队列中最多只能存在一次。
// 系统确保不会有两个 worker 同时处理相同的命名空间。
func (nm *NamespaceController) worker(ctx context.Context) {
	workFunc := func(ctx context.Context) bool {
		key, quit := nm.queue.Get()
		if quit {
			return true
		}
		defer nm.queue.Done(key)

		err := nm.syncNamespaceFromKey(ctx, key)
		if err == nil {
			// no error, forget this entry and return
			nm.queue.Forget(key)
			return false
		}
		// 删除出错了
		// 1.不是出错，是estimate的值大于 0，代表 finalizer 还没执行完，就再入一次队
		if estimate, ok := err.(*deletion.ResourcesRemainingError); ok {
			t := estimate.Estimate/2 + 1
			klog.FromContext(ctx).V(4).Info("Content remaining in namespace", "namespace", key, "waitSeconds", t)
			nm.queue.AddAfter(key, time.Duration(t)*time.Second)
		} else {
			// 2. 出错，也是入队
			// rather than wait for a full resync, re-add the namespace to the queue to be processed
			nm.queue.AddRateLimited(key)
			utilruntime.HandleError(fmt.Errorf("deletion of namespace %v failed: %v", key, err))
		}
		return false
	}
	for {
		quit := workFunc(ctx)

		if quit {
			return
		}
	}
}

// syncNamespaceFromKey looks for a namespace with the specified key in its store and synchronizes it
func (nm *NamespaceController) syncNamespaceFromKey(ctx context.Context, key string) (err error) {
	startTime := time.Now()
	logger := klog.FromContext(ctx)
	defer func() {
		logger.V(4).Info("Finished syncing namespace", "namespace", key, "duration", time.Since(startTime))
	}()
	namespace, err := nm.lister.Get(key)
	// 如果已经在本地缓存都被删除了，说明删除的流程已经走完了
	if errors.IsNotFound(err) {
		logger.Info("Namespace has been deleted", "namespace", key)
		return nil
	}
	// 有未知错误，返回
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Unable to retrieve namespace %v from store: %v", key, err))
		return err
	}
	// 没有被删除 去删除
	return nm.namespacedResourcesDeleter.Delete(ctx, namespace.Name)
}

// Run starts observing the system with the specified number of workers.
func (nm *NamespaceController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()
	defer nm.queue.ShutDown()
	logger := klog.FromContext(ctx)
	logger.Info("Starting namespace controller")
	defer logger.Info("Shutting down namespace controller")

	if !cache.WaitForNamedCacheSync("namespace", ctx.Done(), nm.listerSynced) {
		return
	}

	logger.V(5).Info("Starting workers of namespace controller")
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, nm.worker, time.Second)
	}
	<-ctx.Done()
}
