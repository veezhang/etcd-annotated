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

package concurrency

import (
	"context"
	"fmt"
	"sync"

	v3 "go.etcd.io/etcd/clientv3"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
)

// Mutex implements the sync Locker interface with etcd
// Mutex 实现的是一个分布式可重入公平锁
type Mutex struct {
	s *Session // Session 用来保持 Lease

	pfx   string // 前缀
	myKey string // Key, 格式为： ${pfx}/${Lease}
	myRev int64  // Revision 修订版/全局版本
	hdr   *pb.ResponseHeader
}

// NewMutex 创建
func NewMutex(s *Session, pfx string) *Mutex {
	return &Mutex{s, pfx + "/", "", -1, nil}
}

// Lock locks the mutex with a cancelable context. If the context is canceled
// while trying to acquire the lock, the mutex tries to clean its stale lock entry.
// Lock 加锁
func (m *Mutex) Lock(ctx context.Context) error {
	s := m.s
	client := m.s.Client()

	// Key, 格式为： ${pfx}/${Lease}
	m.myKey = fmt.Sprintf("%s%x", m.pfx, s.Lease())
	// cmp 用于判断 myKey 是否存在
	// Revision 是 etcd 一个全局的序列号，全局唯一且递增，每一个对 etcd 存储进行改动都会分配一个。当 ke y不存在时，为 0 。
	cmp := v3.Compare(v3.CreateRevision(m.myKey), "=", 0)
	// put self in lock waiters via myKey; oldest waiter holds lock
	// 设置命令
	put := v3.OpPut(m.myKey, "", v3.WithLease(s.Lease()))
	// reuse key in case this session already holds the lock
	// 获取命令
	get := v3.OpGet(m.myKey)
	// fetch current holder to complete uncontended path with only one RPC
	// 获取最早创建者，以 pfx 为前缀来获取， WithFirstCreate() 中会指定 WithPrefix()
	getOwner := v3.OpGet(m.pfx, v3.WithFirstCreate()...)
	// if Revision == 0 { put; getOwner; } else { get; getOwner; }
	resp, err := client.Txn(ctx).If(cmp).Then(put, getOwner).Else(get, getOwner).Commit()
	if err != nil {
		return err
	}
	// 到这里 Key 都已经设置好了，就是看要不要等之前的释放了

	// 本次操作的 Revision
	m.myRev = resp.Header.Revision
	if !resp.Succeeded {
		// 操作失败，则获取 Else 返回的值，即已有的 Revision
		m.myRev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	// if no key on prefix / the minimum rev is key, already hold the lock
	// 如果还没有创建过，或者最小的就是当前的 key ，则已经获取锁了，返回
	// 从这里可以看出来是可重入锁
	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == m.myRev {
		m.hdr = resp.Header
		return nil
	}

	// wait for deletion revisions prior to myKey
	// waitDeletes 等待匹配前缀 m.pfx 并且 createRivision 小于 m.myRev-1 所有 key 被删除
	// 如果没有比当前客户端创建的 key 的 revision 小的 key ，则当前客户端者获得锁
	// 从这里可以看出来是公平锁
	hdr, werr := waitDeletes(ctx, client, m.pfx, m.myRev-1)
	// release lock key if wait failed
	// 如果失败则释放锁
	if werr != nil {
		m.Unlock(client.Ctx())
	} else {
		m.hdr = hdr
	}
	return werr
}

// Unlock 释放锁
func (m *Mutex) Unlock(ctx context.Context) error {
	client := m.s.Client()
	// 删除 key
	if _, err := client.Delete(ctx, m.myKey); err != nil {
		return err
	}
	// 清空 myKey myRev
	m.myKey = "\x00"
	m.myRev = -1
	return nil
}

// IsOwner 判断是否获取到锁了
func (m *Mutex) IsOwner() v3.Cmp {
	return v3.Compare(v3.CreateRevision(m.myKey), "=", m.myRev)
}

// Key 返回 Key
func (m *Mutex) Key() string { return m.myKey }

// Header is the response header received from etcd on acquiring the lock.
func (m *Mutex) Header() *pb.ResponseHeader { return m.hdr }

type lockerMutex struct{ *Mutex }

func (lm *lockerMutex) Lock() {
	client := lm.s.Client()
	if err := lm.Mutex.Lock(client.Ctx()); err != nil {
		panic(err)
	}
}
func (lm *lockerMutex) Unlock() {
	client := lm.s.Client()
	if err := lm.Mutex.Unlock(client.Ctx()); err != nil {
		panic(err)
	}
}

// NewLocker creates a sync.Locker backed by an etcd mutex.
// NewLocker 返回实现 sync.Locker 接口的 *lockerMutex
func NewLocker(s *Session, pfx string) sync.Locker {
	return &lockerMutex{NewMutex(s, pfx)}
}
