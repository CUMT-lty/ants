// Copyright 2019 Andy Pan & Dietoad. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sync

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// spinLock 是 ants 自己写的自旋锁
// 自旋锁的定义：自旋锁就是忙等锁，和非自旋锁不同，它并不会彻底放弃 cpu 时间片，而是通过不断自选一直尝试获取锁，直到成功
// 非自旋锁如果获取不到锁，就会将线程阻塞，直到被唤醒
// 自旋锁的适用场景：小范围的原子操作，单个线程持有锁的时间很短，使用自旋锁能够免去频繁的因为线程切换而导致的上下文切换，效率更高

type spinLock uint32 // 自定义的自旋锁

const maxBackoff = 16

func (sl *spinLock) Lock() {
	backoff := 1
	for !atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1) {
		// Leverage the exponential backoff algorithm, see https://en.wikipedia.org/wiki/Exponential_backoff.
		for i := 0; i < backoff; i++ {
			runtime.Gosched() // 让出当前协程的时间片，重新交给 GMP 调度，何时这个协程会恢复调度其实是不确定的
		}
		if backoff < maxBackoff { // 指数退避，如果获取不到锁，那么让出时间片的时间指数级增加
			backoff <<= 1
		}
		// 指数退避的好处：
		// 如果机器 CPU 核心数很多，能保证 runtime.Gosched() 后又马上能够绑定到新的 CPU 核心，占用时间片
		// 那这就和直接 CAS 轮询锁状态没什么区别了，这种情况下就会导致锁冲突次数过多
		// 所以使用指数退避
		// 但是 maxBackoff 的值不能太大，不然性能就慢慢退化为普通互斥锁了
	}
}

func (sl *spinLock) Unlock() {
	atomic.StoreUint32((*uint32)(sl), 0)
}

// NewSpinLock instantiates a spin-lock.
func NewSpinLock() sync.Locker {
	return new(spinLock)
}
