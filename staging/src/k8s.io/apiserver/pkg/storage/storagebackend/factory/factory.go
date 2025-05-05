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

package factory

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// DestroyFunc is to destroy any resources used by the storage returned in Create() together.
type DestroyFunc func()

// Create creates a storage backend based on given config.
// etcd2 和 etcd3 是 etcd 系列的两个主要版本，它们在 Kubernetes 存储中的使用方式有较大的差异，
// 尤其体现在数据模型、性能、功能支持上。Kubernetes 从 1.13 起移除对 etcd2 的支持，全面切换到 etcd3。
// ✅ 主要区别一览：
// 特性	etcd2	etcd3
// 数据模型	基于平面 key-value	支持事务的 MVCC（多版本并发控制）
// Watch 机制	非增量，容易丢事件	增量 Watch，稳定可靠
// 事务支持	不支持原子事务	支持事务（Txn）操作
// 性能与效率	单一版本，效率低	高性能，支持并发读写
// 存储结构	BoltDB，单版本	BoltDB + MVCC，多版本
// Compaction	简单删除旧 key	支持版本压缩、历史清理
// Snapshot / Backup 支持	基础	完善，支持增量快照
// Kubernetes 支持情况	已废弃	官方推荐，默认使用
func Create(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3Storage(c, newFunc, newListFunc, resourcePrefix)
	default:
		return nil, nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// CreateHealthCheck creates a healthcheck function based on given config.
func CreateHealthCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3HealthCheck(c, stopCh)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

func CreateReadyCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ReadyCheck(c, stopCh)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

func CreateProber(c storagebackend.Config) (Prober, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ProberMonitor(c)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

func CreateMonitor(c storagebackend.Config) (metrics.Monitor, error) {
	switch c.Type {
	case storagebackend.StorageTypeETCD2:
		return nil, fmt.Errorf("%s is no longer a supported storage backend", c.Type)
	case storagebackend.StorageTypeUnset, storagebackend.StorageTypeETCD3:
		return newETCD3ProberMonitor(c)
	default:
		return nil, fmt.Errorf("unknown storage type: %s", c.Type)
	}
}

// Prober is an interface that defines the Probe function for doing etcd readiness/liveness checks.
type Prober interface {
	Probe(ctx context.Context) error
	Close() error
}
