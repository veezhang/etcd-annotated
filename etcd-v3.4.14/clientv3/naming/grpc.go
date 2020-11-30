// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package naming

import (
	"context"
	"encoding/json"
	"fmt"

	etcd "go.etcd.io/etcd/clientv3"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/naming"
	"google.golang.org/grpc/status"
)

var ErrWatcherClosed = fmt.Errorf("naming: watch closed")

// GRPCResolver creates a grpc.Watcher for a target to track its resolution changes.
// GRPCResolver 实现 grpc.naming.Resolver 接口，创建 grpc.Watcher 跟踪目标服务的改变
type GRPCResolver struct {
	// Client is an initialized etcd client.
	Client *etcd.Client
}

// Update 更新
//
// grpc.naming.Update 的结构如下
// 	type Update struct {
// 		Op Operation         // 操作
// 		Addr string          // 地址
// 		Metadata interface{} // 元数据
// }
func (gr *GRPCResolver) Update(ctx context.Context, target string, nm naming.Update, opts ...etcd.OpOption) (err error) {
	switch nm.Op {
	case naming.Add:
		var v []byte
		if v, err = json.Marshal(nm); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		_, err = gr.Client.KV.Put(ctx, target+"/"+nm.Addr, string(v), opts...)
	case naming.Delete:
		_, err = gr.Client.Delete(ctx, target+"/"+nm.Addr, opts...)
	default:
		return status.Error(codes.InvalidArgument, "naming: bad naming op")
	}
	return err
}

// Resolve 创建目标服务监视器
func (gr *GRPCResolver) Resolve(target string) (naming.Watcher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	w := &gRPCWatcher{c: gr.Client, target: target + "/", ctx: ctx, cancel: cancel}
	return w, nil
}

// gRPCWatcher 监控特定服务的更新
type gRPCWatcher struct {
	c      *etcd.Client       // 客户端
	target string             // 目标服务
	ctx    context.Context    // ctx
	cancel context.CancelFunc // 取消函数
	wch    etcd.WatchChan     // 等待事件通知 chan ( <-chan WatchResponse )
	err    error              // 错误
}

// Next gets the next set of updates from the etcd resolver.
// Calls to Next should be serialized; concurrent calls are not safe since
// there is no way to reconcile the update ordering.
// Next 从 etcd resolver 获取下一组更新。对 Next 的调用应序列化；并发调用并不安全，因为无法协调更新顺序。
// Next 会阻塞，直到发生更新或错误。 它可能会返回一个或多个更新。 第一次调用应获得完整的结果集。 当且仅当 Watcher 无法恢复时，它应该返回错误。
func (gw *gRPCWatcher) Next() ([]*naming.Update, error) {
	if gw.wch == nil {
		// first Next() returns all addresses
		// 第一次，返回所有的地址，使用了延迟操作
		return gw.firstNext()
	}
	// 出错
	if gw.err != nil {
		return nil, gw.err
	}

	// process new events on target/*
	// 处理 target/* 下的事件
	wr, ok := <-gw.wch
	if !ok {
		gw.err = status.Error(codes.Unavailable, ErrWatcherClosed.Error())
		return nil, gw.err
	}
	if gw.err = wr.Err(); gw.err != nil {
		return nil, gw.err
	}

	// 处理多个更新事件，并返回
	updates := make([]*naming.Update, 0, len(wr.Events))
	for _, e := range wr.Events {
		var jupdate naming.Update
		var err error
		switch e.Type {
		case etcd.EventTypePut:
			err = json.Unmarshal(e.Kv.Value, &jupdate)
			jupdate.Op = naming.Add
		case etcd.EventTypeDelete:
			err = json.Unmarshal(e.PrevKv.Value, &jupdate)
			jupdate.Op = naming.Delete
		default:
			continue
		}
		if err == nil {
			updates = append(updates, &jupdate)
		}
	}
	return updates, nil
}

// firstNext 第一次调用 Next 时候的处理
func (gw *gRPCWatcher) firstNext() ([]*naming.Update, error) {
	// Use serialized request so resolution still works if the target etcd
	// server is partitioned away from the quorum.
	// 使用序列化的请求
	// 获取前缀为 target 的值
	resp, err := gw.c.Get(gw.ctx, gw.target, etcd.WithPrefix(), etcd.WithSerializable())
	if gw.err = err; err != nil {
		return nil, err
	}

	// 统计结果
	updates := make([]*naming.Update, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var jupdate naming.Update
		if err := json.Unmarshal(kv.Value, &jupdate); err != nil {
			continue
		}
		updates = append(updates, &jupdate)
	}

	// 然后创建监控，只有第一次调用后才会设置 wch
	opts := []etcd.OpOption{etcd.WithRev(resp.Header.Revision + 1), etcd.WithPrefix(), etcd.WithPrevKV()}
	gw.wch = gw.c.Watch(gw.ctx, gw.target, opts...)
	return updates, nil
}

// Close 关闭监控
func (gw *gRPCWatcher) Close() { gw.cancel() }
