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

package cache

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

// ThreadSafeStore is an interface that allows concurrent indexed
// access to a storage backend.  It is like Indexer but does not
// (necessarily) know how to extract the Store key from a given
// object.
//
// TL;DR caveats: you must not modify anything returned by Get or List as it will break
// the indexing feature in addition to not being thread safe.
//
// The guarantees of thread safety provided by List/Get are only valid if the caller
// treats returned items as read-only. For example, a pointer inserted in the store
// through `Add` will be returned as is by `Get`. Multiple clients might invoke `Get`
// on the same key and modify the pointer in a non-thread-safe way. Also note that
// modifying objects stored by the indexers (if any) will *not* automatically lead
// to a re-index. So it's not a good idea to directly modify the objects returned by
// Get/List, in general.
type ThreadSafeStore interface {
	Add(key string, obj interface{})
	Update(key string, obj interface{})
	Delete(key string)
	Get(key string) (item interface{}, exists bool)
	List() []interface{}
	ListKeys() []string
	Replace(map[string]interface{}, string)
	Index(indexName string, obj interface{}) ([]interface{}, error)
	IndexKeys(indexName, indexedValue string) ([]string, error)
	ListIndexFuncValues(name string) []string
	ByIndex(indexName, indexedValue string) ([]interface{}, error)
	GetIndexers() Indexers

	// AddIndexers adds more indexers to this store. This supports adding indexes after the store already has items.
	AddIndexers(newIndexers Indexers) error
	// Resync is a no-op and is deprecated
	Resync() error
}

// storeIndex implements the indexing functionality for Store interface
type storeIndex struct {
	// indexers maps a name to an IndexFunc
	indexers Indexers
	// indices maps a name to an Index
	indices Indices
}

func (i *storeIndex) reset() {
	i.indices = Indices{}
}

func (i *storeIndex) getKeysFromIndex(indexName string, obj interface{}) (sets.String, error) {
	indexFunc := i.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	indexedValues, err := indexFunc(obj)
	if err != nil {
		return nil, err
	}
	index := i.indices[indexName]

	var storeKeySet sets.String
	if len(indexedValues) == 1 {
		// In majority of cases, there is exactly one value matching.
		// Optimize the most common path - deduping is not needed here.
		storeKeySet = index[indexedValues[0]]
	} else {
		// Need to de-dupe the return list.
		// Since multiple keys are allowed, this can happen.
		storeKeySet = sets.String{}
		for _, indexedValue := range indexedValues {
			for key := range index[indexedValue] {
				storeKeySet.Insert(key)
			}
		}
	}

	return storeKeySet, nil
}

func (i *storeIndex) getKeysByIndex(indexName, indexedValue string) (sets.String, error) {
	indexFunc := i.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	index := i.indices[indexName]
	return index[indexedValue], nil
}

func (i *storeIndex) getIndexValues(indexName string) []string {
	index := i.indices[indexName]
	names := make([]string, 0, len(index))
	for key := range index {
		names = append(names, key)
	}
	return names
}

func (i *storeIndex) addIndexers(newIndexers Indexers) error {
	oldKeys := sets.StringKeySet(i.indexers)
	newKeys := sets.StringKeySet(newIndexers)

	if oldKeys.HasAny(newKeys.List()...) {
		return fmt.Errorf("indexer conflict: %v", oldKeys.Intersection(newKeys))
	}

	for k, v := range newIndexers {
		i.indexers[k] = v
	}
	return nil
}

// updateSingleIndex modifies the objects location in the named index:
// - for create you must provide only the newObj
// - for update you must provide both the oldObj and the newObj
// - for delete you must provide only the oldObj
// updateSingleIndex must be called from a function that already has a lock on the cache
func (i *storeIndex) updateSingleIndex(name string, oldObj interface{}, newObj interface{}, key string) {
	var oldIndexValues, indexValues []string
	indexFunc, ok := i.indexers[name]
	if !ok {
		// Should never happen. Caller is responsible for ensuring this exists, and should call with lock
		// held to avoid any races.
		panic(fmt.Errorf("indexer %q does not exist", name))
	}
	if oldObj != nil {
		var err error
		oldIndexValues, err = indexFunc(oldObj)
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", key, name, err))
		}
	} else {
		oldIndexValues = oldIndexValues[:0]
	}

	if newObj != nil {
		var err error
		indexValues, err = indexFunc(newObj)
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", key, name, err))
		}
	} else {
		indexValues = indexValues[:0]
	}

	index := i.indices[name]
	if index == nil {
		index = Index{}
		i.indices[name] = index
	}

	if len(indexValues) == 1 && len(oldIndexValues) == 1 && indexValues[0] == oldIndexValues[0] {
		// We optimize for the most common case where indexFunc returns a single value which has not been changed
		return
	}

	for _, value := range oldIndexValues {
		i.deleteKeyFromIndex(key, value, index)
	}
	for _, value := range indexValues {
		i.addKeyToIndex(key, value, index)
	}
}

// updateIndices modifies the objects location in the managed indexes:
// - for create you must provide only the newObj
// - for update you must provide both the oldObj and the newObj
// - for delete you must provide only the oldObj
// updateIndices must be called from a function that already has a lock on the cache
func (i *storeIndex) updateIndices(oldObj interface{}, newObj interface{}, key string) {
	for name := range i.indexers {
		i.updateSingleIndex(name, oldObj, newObj, key)
	}
}

func (i *storeIndex) addKeyToIndex(key, indexValue string, index Index) {
	set := index[indexValue]
	if set == nil {
		set = sets.String{}
		index[indexValue] = set
	}
	set.Insert(key)
}

func (i *storeIndex) deleteKeyFromIndex(key, indexValue string, index Index) {
	set := index[indexValue]
	if set == nil {
		return
	}
	set.Delete(key)
	// If we don't delete the set when zero, indices with high cardinality
	// short lived resources can cause memory to increase over time from
	// unused empty sets. See `kubernetes/kubernetes/issues/84959`.
	if len(set) == 0 {
		delete(index, indexValue)
	}
}

// threadSafeMap 中，没有使用 Go 的 sync.Map 而是采用 map + RWMutex 的设计，主要有以下几个原因：
//
// 1. sync.Map 的适用场景
// Go 的 sync.Map 是为特定场景优化的并发安全 Map，适用于：
//
// 读多写少（绝大多数操作是读取，写入很少）。
//
// Key 稳定性高（Key 一旦写入后很少删除或更新）。
//
// 不需要范围查询（sync.Map 不支持按范围遍历）。
//
// 但 Kubernetes 的 threadSafeMap 的使用场景不同：
//
// 频繁写入：Watch 机制会持续接收事件并更新缓存。
//
// 动态 Key：Pod、Service 等资源会频繁创建、删除、更新。
//
// 需要索引和范围查询：Kubernetes 需要支持 List、Watch 等范围操作。
//
// 2. map + RWMutex 的优势
// (1) 更可控的锁粒度
// threadSafeMap 使用 sync.RWMutex，可以精细控制 读锁（RLock）和写锁（Lock）。
//
// 而 sync.Map 内部使用更复杂的无锁+锁混合机制，在频繁写入时性能可能下降。
//
// (2) 支持索引和范围查询
// threadSafeMap 需要维护额外的索引（如 namespace、labels），而 sync.Map 无法直接支持这种复杂查询。
//
// 例如：
//
// go
// // 按 namespace 查询 Pod
// pods, err := store.ByIndex("namespace", "default")
// 这种操作在 sync.Map 上难以高效实现。
//
// (3) 内存效率更高
// sync.Map 为了实现无锁读取，内部使用了 冗余存储（如 read 和 dirty 两个 Map），内存占用更高。
//
// map + RWMutex 更节省内存，适合 Kubernetes 缓存大量资源的场景。
//
// (4) 更低的 GC 压力
// sync.Map 内部使用 atomic.Value 和接口类型，可能增加 Go GC 的负担。
//
// map + RWMutex 直接存储具体类型，GC 更高效。
//
// 3. sync.Map 的性能对比
// 场景	map + RWMutex	sync.Map
// 读多写少	一般（需加读锁）	⭐️ 最优（无锁读）
// 写多读少	⭐️ 较好（可控锁）	较差（锁竞争+冗余拷贝）
// 频繁更新/删除	⭐️ 较好	较差（需要清理 dirty Map）
// 范围查询	⭐️ 支持	不支持
// 内存占用	⭐️ 较低	较高（维护两份数据）
// threadSafeMap implements ThreadSafeStore
type threadSafeMap struct {
	lock sync.RWMutex
	//items["default/pod-1"] = &Pod{
	//    Name:      "pod-1",
	//    Namespace: "default",
	//    Containers: []Container{ /* ... */ },
	//}
	items map[string]interface{}
	// index implements the indexing functionality
	index *storeIndex
}

func (c *threadSafeMap) Add(key string, obj interface{}) {
	c.Update(key, obj)
}

func (c *threadSafeMap) Update(key string, obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	oldObject := c.items[key]
	c.items[key] = obj
	c.index.updateIndices(oldObject, obj, key)
}

func (c *threadSafeMap) Delete(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if obj, exists := c.items[key]; exists {
		c.index.updateIndices(obj, nil, key)
		delete(c.items, key)
	}
}

func (c *threadSafeMap) Get(key string) (item interface{}, exists bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	item, exists = c.items[key]
	return item, exists
}

func (c *threadSafeMap) List() []interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()
	list := make([]interface{}, 0, len(c.items))
	for _, item := range c.items {
		list = append(list, item)
	}
	return list
}

// ListKeys returns a list of all the keys of the objects currently
// in the threadSafeMap.
func (c *threadSafeMap) ListKeys() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	list := make([]string, 0, len(c.items))
	for key := range c.items {
		list = append(list, key)
	}
	return list
}

func (c *threadSafeMap) Replace(items map[string]interface{}, resourceVersion string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.items = items

	// rebuild any index
	c.index.reset()
	for key, item := range c.items {
		c.index.updateIndices(nil, item, key)
	}
}

// Index returns a list of items that match the given object on the index function.
// Index is thread-safe so long as you treat all items as immutable.
func (c *threadSafeMap) Index(indexName string, obj interface{}) ([]interface{}, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	storeKeySet, err := c.index.getKeysFromIndex(indexName, obj)
	if err != nil {
		return nil, err
	}

	list := make([]interface{}, 0, storeKeySet.Len())
	for storeKey := range storeKeySet {
		list = append(list, c.items[storeKey])
	}
	return list, nil
}

// ByIndex returns a list of the items whose indexed values in the given index include the given indexed value
func (c *threadSafeMap) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	set, err := c.index.getKeysByIndex(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	list := make([]interface{}, 0, set.Len())
	for key := range set {
		list = append(list, c.items[key])
	}

	return list, nil
}

// IndexKeys returns a list of the Store keys of the objects whose indexed values in the given index include the given indexed value.
// IndexKeys is thread-safe so long as you treat all items as immutable.
func (c *threadSafeMap) IndexKeys(indexName, indexedValue string) ([]string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	set, err := c.index.getKeysByIndex(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	return set.List(), nil
}

func (c *threadSafeMap) ListIndexFuncValues(indexName string) []string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.index.getIndexValues(indexName)
}

func (c *threadSafeMap) GetIndexers() Indexers {
	return c.index.indexers
}

func (c *threadSafeMap) AddIndexers(newIndexers Indexers) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.index.addIndexers(newIndexers); err != nil {
		return err
	}

	// If there are already items, index them
	for key, item := range c.items {
		for name := range newIndexers {
			c.index.updateSingleIndex(name, nil, item, key)
		}
	}

	return nil
}

func (c *threadSafeMap) Resync() error {
	// Nothing to do
	return nil
}

// NewThreadSafeStore creates a new instance of ThreadSafeStore.
func NewThreadSafeStore(indexers Indexers, indices Indices) ThreadSafeStore {
	return &threadSafeMap{
		items: map[string]interface{}{},
		index: &storeIndex{
			indexers: indexers,
			indices:  indices,
		},
	}
}
