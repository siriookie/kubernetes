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

package etcd3

import (
	"context"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"
)

const (
	compactRevKey = "compact_rev_key"
)

var (
	endpointsMapMu sync.Mutex
	endpointsMap   map[string]struct{}
)

func init() {
	endpointsMap = make(map[string]struct{})
}

// StartCompactor starts a compactor in the background to compact old version of keys that's not needed.
// By default, we save the most recent 5 minutes data and compact versions > 5minutes ago.
// It should be enough for slow watchers and to tolerate burst.
// etcd v3 的 Compactor（压缩器）的启动逻辑，主要用于 定期清理 etcd 中过期的旧版本数据，以节省存储空间并提升性能
// 默认保留 最近 5 分钟的数据，压缩 5 分钟前的旧版本（实际压缩逻辑在 compactor 函数中实现，未在此片段展示）。
//
// 未来可能扩展为保留 12 小时历史（需存储层支持）。
// TODO: We might keep a longer history (12h) in the future once storage API can take advantage of past version of keys.
func StartCompactor(ctx context.Context, client *clientv3.Client, compactInterval time.Duration) {
	endpointsMapMu.Lock()
	defer endpointsMapMu.Unlock()

	// In one process, we can have only one compactor for one cluster.
	// Currently we rely on endpoints to differentiate clusters.
	//多 API Server 的问题
	//如果有多个 kube-apiserver 实例（例如 Kubernetes 高可用部署），每个实例都会独立调用 StartCompactor，导致：
	//
	//每个进程都会启动自己的 Compactor 协程。
	//
	//多个 Compactor 同时运行，可能对同一个 etcd 集群重复压缩。
	//尽管存在多进程竞态，实际生产中可能不会引发严重问题，原因如下：
	//
	//（1）etcd 的压缩是幂等的
	//etcd 的压缩操作（Compact）本质是 提交一个目标修订版本（revision），要求删除比该版本更早的历史数据。
	//
	//如果多个 Compactor 同时提交相同的或更早的 revision，etcd 会忽略冗余请求（压缩只会向前推进）。
	//
	//（2）压缩间隔通常较长
	//默认压缩间隔（如 5 分钟）远大于 API Server 的启动时间差，即使多个 Compactor 运行，实际触发频率不会显著增加。
	//
	//（3）资源竞争有限
	//压缩操作是轻量级的后台任务，不会频繁占用大量 CPU/IO。
	for _, ep := range client.Endpoints() {
		if _, ok := endpointsMap[ep]; ok {
			klog.V(4).Infof("compactor already exists for endpoints %v", client.Endpoints())
			return
		}
	}
	//将当前客户端的 etcd 端点（如 http://127.0.0.1:2379）加入 endpointsMap，标记为“已启动 Compactor”。
	for _, ep := range client.Endpoints() {
		endpointsMap[ep] = struct{}{}
	}

	if compactInterval != 0 {
		go compactor(ctx, client, compactInterval)
	}
}

// compactor periodically compacts historical versions of keys in etcd.
// It will compact keys with versions older than given interval.
// In other words, after compaction, it will only contain keys set during last interval.
// Any API call for the older versions of keys will return error.
// Interval is the time interval between each compaction. The first compaction happens after "interval".
// ）compactor 函数（主循环）
// 作用：每隔 interval 时间（默认 5 分钟）触发一次压缩操作。
//
// 关键流程：
//
// 定时触发：通过 time.After(interval) 或 ctx.Done()（优雅退出）控制循环。
//
// 调用 compact：执行实际的压缩逻辑，传入当前的 compactTime（压缩时间戳）和 rev（etcd 全局修订版本号）。
//
// 错误处理：若压缩失败，记录日志并继续下一轮循环。
func compactor(ctx context.Context, client *clientv3.Client, interval time.Duration) {
	// Technical definitions:
	// We have a special key in etcd defined as *compactRevKey*.
	// compactRevKey's value will be set to the string of last compacted revision.
	// compactRevKey's version will be used as logical time for comparison. THe version is referred as compact time.
	// Initially, because the key doesn't exist, the compact time (version) is 0.
	//
	// Algorithm:
	// - Compare to see if (local compact_time) = (remote compact_time).
	// - If yes, increment both local and remote compact_time, and do a compaction.
	// - If not, set local to remote compact_time.
	//
	// Technical details/insights:
	//
	// The protocol here is lease based. If one compactor CAS successfully, the others would know it when they fail in
	// CAS later and would try again in 5 minutes. If an APIServer crashed, another one would "take over" the lease.
	//
	// For example, in the following diagram, we have a compactor C1 doing compaction in t1, t2. Another compactor C2
	// at t1' (t1 < t1' < t2) would CAS fail, set its known oldRev to rev at t1', and try again in t2' (t2' > t2).
	// If C1 crashed and wouldn't compact at t2, C2 would CAS successfully at t2'.
	//
	//                 oldRev(t2)     curRev(t2)
	//                                  +
	//   oldRev        curRev           |
	//     +             +              |
	//     |             |              |
	//     |             |    t1'       |     t2'
	// +---v-------------v----^---------v------^---->
	//     t0           t1             t2
	//
	// We have the guarantees:
	// - in normal cases, the interval is 5 minutes.
	// - in failover, the interval is >5m and <10m
	//
	// FAQ:
	// - What if time is not accurate? We don't care as long as someone did the compaction. Atomicity is ensured using
	//   etcd API.
	// - What happened under heavy load scenarios? Initially, each apiserver will do only one compaction
	//   every 5 minutes. This is very unlikely affecting or affected w.r.t. server load.
	//// 常见问题解答：
	////- 如果时间不准确会怎样？只要有人执行了压缩操作，我们就不在乎时间是否准确。通过 etcd API 来确保原子性。
	////- 在高负载场景下会发生什么情况？最初，每个 API 服务器每 5 分钟只会执行一次压缩操作。就服务器负载而言，这种情况极不可能对服务器造成影响，也不太可能受到服务器负载的影响。
	var compactTime int64
	var rev int64
	var err error
	for {
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return
		}
		//压缩逻辑（compact 函数）
		//原子事务（CAS）：
		//
		//go
		//resp, err := client.KV.Txn(ctx).If(
		//    clientv3.Compare(clientv3.Version(compactRevKey), "=", t), // 检查版本是否匹配
		//).Then(
		//    clientv3.OpPut(compactRevKey, strconv.FormatInt(rev, 10)), // 更新键值并递增版本
		//).Else(
		//    clientv3.OpGet(compactRevKey), // 获取当前状态
		//).Commit()
		//成功：当前压缩器获得执行权，调用 client.Compact 清理历史数据。
		//
		//失败：其他压缩器已执行操作，同步最新状态后重试。
		//
		//4. 容错性
		//故障转移：
		//
		//若压缩器崩溃，其他实例会在下次间隔（5 分钟后）检测并接管压缩任务。
		//
		//幂等性：
		//
		//相同 revision 的压缩操作只会生效一次。
		//
		//工作示例
		//场景：两个压缩器（C1 和 C2）
		//初始状态
		//
		//compactRevKey 不存在，compactTime = 0。
		//
		//C1 首次压缩（t1）
		//
		//CAS 成功，设置 compactRevKey 的版本为 1，执行压缩。
		//
		//C2 尝试压缩（t1'）
		//
		//CAS 失败（版本已变为 1），同步 compactTime = 1 后等待。
		//
		//C1 崩溃
		//
		//C2 在 t2' 时检测到无更新，接管压缩任务。
		compactTime, rev, err = compact(ctx, client, compactTime, rev)
		if err != nil {
			klog.Errorf("etcd: endpoint (%v) compact failed: %v", client.Endpoints(), err)
			continue
		}
	}
}

// compact compacts etcd store and returns current rev.
// It will return the current compact time and global revision if no error occurred.
// Note that CAS fail will not incur any error.
func compact(ctx context.Context, client *clientv3.Client, t, rev int64) (int64, int64, error) {
	// 使用etcd客户端的事务（Txn）机制
	resp, err := client.KV.Txn(ctx).If(
		// 比较compactRevKey的版本号是否等于t
		clientv3.Compare(clientv3.Version(compactRevKey), "=", t),
	).Then(
		// 如果比较成功，将rev转换为字符串并写入compactRevKey
		clientv3.OpPut(compactRevKey, strconv.FormatInt(rev, 10)), // Expect side effect: increment Version
	).Else(
		// 如果比较失败，获取compactRevKey的值
		clientv3.OpGet(compactRevKey),
	).Commit()
	// 若事务执行出错，返回原始的t、rev和错误信息
	if err != nil {
		return t, rev, err
	}

	// 获取响应头中的全局修订版本号
	curRev := resp.Header.Revision

	// 若事务未成功
	if !resp.Succeeded {
		// 从响应中获取当前的压缩时间
		curTime := resp.Responses[0].GetResponseRange().Kvs[0].Version
		return curTime, curRev, nil
	}
	// 若事务成功，将当前压缩时间加1
	curTime := t + 1

	// 如果rev为0，不进行压缩操作，直接返回当前时间和全局修订版本号
	if rev == 0 {
		// We don't compact on bootstrap.
		return curTime, curRev, nil
	}
	// 执行etcd的压缩操作
	if _, err = client.Compact(ctx, rev); err != nil {
		return curTime, curRev, err
	}
	// 记录压缩操作的日志信息
	klog.V(4).Infof("etcd: compacted rev (%d), endpoints (%v)", rev, client.Endpoints())
	return curTime, curRev, nil
}
