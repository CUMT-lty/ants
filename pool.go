// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ants

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	syncx "github.com/panjf2000/ants/v2/internal/sync" // 自定义的自旋锁
)

// Pool accepts the tasks and process them concurrently,
// it limits the total of goroutines to a given number by recycling goroutines.
type Pool struct {
	// capacity of the pool, a negative value means that the capacity of pool is limitless, an infinite pool is used to
	// avoid potential issue of endless blocking caused by nested usage of a pool: submitting a task to pool
	// which submits a new task to the same pool.
	capacity int32

	// running is the number of the currently running goroutines.
	running int32

	// lock for protecting the worker queue.
	lock sync.Locker // spinlock 自旋锁，用于并发安全的从worker队列中获取空闲worker

	// workers is a slice that store the available workers.
	workers workerQueue // workers 中的 worker 都是已经被创建，挂在 run 处等待执行任务的（那些正在执行任务的 worker 不在队列上）
	// 也就是说只有没有任务可执行，一直阻塞在 run 处的 worker 才是需要被清理的 worker

	// state is used to notice the pool to closed itself.
	state int32 // 标记当前 pool 的状态：开启或者关闭

	// cond for waiting to get an idle worker.
	cond *sync.Cond // 标识当前是否有空闲 worker

	// workerCache speeds up the obtainment of a usable worker in function:retrieveWorker.
	workerCache sync.Pool // workers 中的 worker 也是从 workerCache 中来的
	// retrieveWorker 获取 worker 的逻辑是先尝试从 workers 队列中获取
	// 如果没有，但还没达到设定的池容量上限，那就再创建一个 worker
	// 但是不自己创建，而是使用 sync.Pool 即 workerCache 来创建
	// TODO: 清理 worker 的时候应该也也是从队列归还到 workerCache 中

	// waiting is the number of goroutines already been blocked on pool.Submit(), protected by pool.lock
	waiting int32

	// TODO: 从 Reboot 方法的逻辑来看，标识一个 pool 状态的要素：
	// 1.state
	// 2.purgeStaleWorkers 协程是否在执行
	// 3.ticktock 协程是否在执行

	purgeDone int32              // 标识 p.purgeStaleWorkers 线程是否还在运行
	stopPurge context.CancelFunc // 用来使 p.purgeStaleWorkers(ctx) 协程退出的 cancel 方法

	ticktockDone int32              // 标识 p.ticktock 线程是否还在运行
	stopTicktock context.CancelFunc // 用来使 p.ticktock(ctx) 协程退出的 cancel 方法

	now atomic.Value

	options *Options
}

// purgeStaleWorkers clears stale workers periodically, it runs in an individual goroutine, as a scavenger.
func (p *Pool) purgeStaleWorkers(ctx context.Context) {
	ticker := time.NewTicker(p.options.ExpiryDuration)

	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&p.purgeDone, 1)
	}()

	for {
		select { // 非阻塞式检查一下 scavenger 协程是否被 cancel() 了
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// 注意，这里 select 中没有设置 default 分支，所以整个 select 是阻塞式的
		// 如果这个 scavenger 协程没有被 cancel() 掉，就会等计时器信号，来实现定时任务

		if p.IsClosed() {
			break
		}

		var isDormant bool // 是否在休眠
		p.lock.Lock()
		// 将过期 worker 整理出来放在 expire 切片中并返回
		// 这些 worker 还没有真的被清理掉，内部的 task 也在继续被执行
		staleWorkers := p.workers.refresh(p.options.ExpiryDuration)
		n := p.Running() // 获取当前正在工作的线程数量
		// running 的 worker 有两种：
		// 1.被从 workerQueue 中摘下来，正在执行任务，没有阻塞
		// 2.被归还到 workerQueue 中，阻塞在 run 处，在等任务
		// 只有一个 worker 过期被清理了，归还到 workerCache 中，running 才会 -1
		isDormant = n == 0 || n == len(staleWorkers) // 如果当前没有正在工作的 worker 了，就设置休眠标志
		p.lock.Unlock()

		// Notify obsolete workers to stop.
		// This notification must be outside the p.lock, since w.task
		// may be blocking and may consume a lot of time if many workers
		// are located on non-local CPUs.
		// TODO: 必须在解锁后执行，因为 w.task 可能是一个阻塞的 chan
		// TODO：如果在锁的内部且很多 worker 位于非本地 CPU 上的时候就会花费大量的时间
		// 考虑下面这一小段 for 循环代码的并发安全问题：
		// 因为是在 purge 协程内部，单个 pool 只会开一个 purge 协程，所以这段代码不会和任何其他 purge 协程有并发冲突
		// 这段代码操作的是 workerQueue 中的 expire 部分，其他可能并发的协程操作的都是 items 部分，所以和其他可能并发的协程也没有冲突
		// 所以从正确性角度来讲，这一小段代码放在锁内和锁外都没有问题
		// TODO: 但是问题在于，这里使用的锁，和 cond 绑定的锁，是同一把锁
		// 如果在锁外部，一个 worker 退出完成，就可以唤醒一个阻塞在 retrieveWorker 去继续获取 worker 执行任务，这是异步并发的逻辑
		// 如果这段代码写在锁内部，retrieveWorker 中 wait 结束阻塞的时候，会重新获取锁然后再去执行后续代码
		// 但是此时还没有解锁，所以即使唤醒了一个 retrieveWorker 协程，在 wait() 即将退出，要获取锁的时候还是获取不到，那上述过程就不是异步的了
		// 就需要等 expire 中的所有 worker 退出完成，解锁，然后 retrieveWorker 协程才能继续执行，这中间是有时间损耗的
		// expire 中的 worker 越多，这个时间损耗越大
		for i := range staleWorkers { // 通知所有过期的 worker 停止工作
			// 就是向 worker 的 task chan 中塞一个 nil
			// 这些 worker 本来是阻塞在 run 处的，读到 nil 就会自己退出
			staleWorkers[i].finish()
			staleWorkers[i] = nil
		}

		// There might be a situation where all workers have been cleaned up (no worker is running),
		// while some invokers still are stuck in p.cond.Wait(), then we need to awake those invokers.
		if isDormant && p.Waiting() > 0 { // 如果当前没有正在运行的 worker，也就是所有的 worker 都被归还到 workerCache 中了
			p.cond.Broadcast() // 广播唤醒所有阻塞在 retrieveWorker 中要获取 worker 的协程
		}
	}
}

// ticktock is a goroutine that updates the current time in the pool regularly.
func (p *Pool) ticktock(ctx context.Context) {
	ticker := time.NewTicker(nowTimeUpdateInterval)
	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&p.ticktockDone, 1)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if p.IsClosed() {
			break
		}

		p.now.Store(time.Now())
	}
}

func (p *Pool) goPurge() { // 负责启一个 purgeStaleWorkers 协程
	if p.options.DisablePurge { // 如果不需要清理 worker，直接返回
		return
	}

	// Start a goroutine to clean up expired workers periodically.
	var ctx context.Context
	ctx, p.stopPurge = context.WithCancel(context.Background())
	go p.purgeStaleWorkers(ctx)
}

func (p *Pool) goTicktock() { // 负责启一个 ticktock 协程
	p.now.Store(time.Now())
	var ctx context.Context
	ctx, p.stopTicktock = context.WithCancel(context.Background())
	go p.ticktock(ctx)
}

func (p *Pool) nowTime() time.Time {
	return p.now.Load().(time.Time)
}

// NewPool instantiates a Pool with customized options.
func NewPool(size int, options ...Option) (*Pool, error) {
	if size <= 0 {
		size = -1
	}

	opts := loadOptions(options...)

	if !opts.DisablePurge { // 如果需要定期清理 worker
		if expiry := opts.ExpiryDuration; expiry < 0 { // TODO
			return nil, ErrInvalidPoolExpiry
		} else if expiry == 0 {
			opts.ExpiryDuration = DefaultCleanIntervalTime
		}
	}

	if opts.Logger == nil {
		opts.Logger = defaultLogger
	}

	p := &Pool{
		capacity: int32(size),
		lock:     syncx.NewSpinLock(), // TODO：是自定义的自旋锁，标准库中留了接口可以自定义锁
		options:  opts,
	}
	p.workerCache.New = func() interface{} { // sync.Pool 的用法，定义 New 函数
		return &goWorker{
			pool: p,
			task: make(chan func(), workerChanCap), // 缓冲为 0 或 1，由 GOMAXPROCS 的值决定（点进去看 workerChanCap 方法）
		}
	}
	if p.options.PreAlloc { // 预分配内存
		if size == -1 {
			return nil, ErrInvalidPreAllocSize
		}
		p.workers = newWorkerQueue(queueTypeLoopQueue, size) // 如果预分配内存就以循环队列方式创建
	} else {
		p.workers = newWorkerQueue(queueTypeStack, 0) // 如果不预分配内存就以栈方式创建
	}

	p.cond = sync.NewCond(p.lock) // pool.Cond 必须要绑定一个锁，wait() 前必须加锁，wait 后必须释放锁

	// 启两个后台协程
	p.goPurge()    // 启一个清理协程，周期性清理协程池中的协程
	p.goTicktock() // 启一个计时协程，周期性更新当前时间

	return p, nil
}

// Submit submits a task to this pool.
//
// Note that you are allowed to call Pool.Submit() from the current Pool.Submit(),
// but what calls for special attention is that you will get blocked with the last
// Pool.Submit() call once the current Pool runs out of its capacity, and to avoid this,
// you should instantiate a Pool with ants.WithNonblocking(true).
func (p *Pool) Submit(task func()) error {
	if p.IsClosed() {
		return ErrPoolClosed
	}

	w, err := p.retrieveWorker() // 获取一个可用的 worker（并保证该 worker run 起来能够去消费 task）
	if w != nil {
		w.inputFunc(task) // 将任务放到该 worker 的 chan 中
	}
	return err
}

// Running returns the number of workers currently running.
func (p *Pool) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

// Free returns the number of available workers, -1 indicates this pool is unlimited.
func (p *Pool) Free() int {
	c := p.Cap()
	if c < 0 {
		return -1
	}
	return c - p.Running() // Cap() = Running() + Free()
}

// Waiting returns the number of tasks waiting to be executed.
func (p *Pool) Waiting() int {
	return int(atomic.LoadInt32(&p.waiting))
}

// Cap returns the capacity of this pool.
func (p *Pool) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

// Tune changes the capacity of this pool, note that it is noneffective to the infinite or pre-allocation pool.
func (p *Pool) Tune(size int) {
	capacity := p.Cap()
	if capacity == -1 || size <= 0 || size == capacity || p.options.PreAlloc {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
	if size > capacity {
		if size-capacity == 1 { // 如果只多出来一个
			p.cond.Signal() // 只唤醒一个想要获得空闲 worker 的线程
			return
		}
		p.cond.Broadcast() // 如果多出来多个就广播
	}
}

// IsClosed indicates whether the pool is closed.
func (p *Pool) IsClosed() bool {
	return atomic.LoadInt32(&p.state) == CLOSED
}

// Release closes this pool and releases the worker queue.
func (p *Pool) Release() {
	if !atomic.CompareAndSwapInt32(&p.state, OPENED, CLOSED) {
		return
	}

	if p.stopPurge != nil { // 只 cancel 一次
		p.stopPurge()
		p.stopPurge = nil
	}
	p.stopTicktock()
	p.stopTicktock = nil

	p.lock.Lock()
	p.workers.reset() // 结束 workerQueue 中的所有 worker 并将 worker 置空
	p.lock.Unlock()
	// There might be some callers waiting in retrieveWorker(), so we need to wake them up to prevent
	// those callers blocking infinitely.
	p.cond.Broadcast() // 广播唤醒所有阻塞的调用者
}

// ReleaseTimeout is like Release but with a timeout, it waits all workers to exit before timing out.
func (p *Pool) ReleaseTimeout(timeout time.Duration) error { // 等待所有的 worker 在超时时间前全部退出
	if p.IsClosed() || (!p.options.DisablePurge && p.stopPurge == nil) || p.stopTicktock == nil {
		return ErrPoolClosed
	}
	p.Release()

	endTime := time.Now().Add(timeout)
	for time.Now().Before(endTime) {
		if p.Running() == 0 &&
			(p.options.DisablePurge || atomic.LoadInt32(&p.purgeDone) == 1) &&
			atomic.LoadInt32(&p.ticktockDone) == 1 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ErrTimeout
}

// Reboot reboots a closed pool.
func (p *Pool) Reboot() {
	if atomic.CompareAndSwapInt32(&p.state, CLOSED, OPENED) {
		atomic.StoreInt32(&p.purgeDone, 0)
		p.goPurge()
		atomic.StoreInt32(&p.ticktockDone, 0)
		p.goTicktock()
	}
}

func (p *Pool) addRunning(delta int) {
	atomic.AddInt32(&p.running, int32(delta))
}

func (p *Pool) addWaiting(delta int) {
	atomic.AddInt32(&p.waiting, int32(delta))
}

// retrieveWorker returns an available worker to run the tasks.
func (p *Pool) retrieveWorker() (w worker, err error) {
	p.lock.Lock()

retry:
	// First try to fetch the worker from the queue.
	// 先尝试从 workerQueue 中获取，workerQueue 中的 worker 都是已经启动过的，阻塞在 run 处等待任务的
	if w = p.workers.detach(); w != nil {
		p.lock.Unlock()
		// 注意和下面从 cache 中获取的 worker 不同，这里不需要 worker.run
		// 因为队列中没有被清理的 worker 还在 run
		return
	}

	// If the worker queue is empty, and we don't run out of the pool capacity,
	// then just spawn a new worker goroutine.
	// 如果没有 workerQueue 中没有，再创建新的 worker
	// 这个创建不是自己创建，而是交给 workerCache 去创建（worker 的创建和回收都交给 sync.Pool 来管理）
	if capacity := p.Cap(); capacity == -1 || capacity > p.Running() { // 如果创建的 worker 数量还没达到设定的池容量
		p.lock.Unlock()
		w = p.workerCache.Get().(*goWorker)
		w.run() // 这里需要 w.run() 让这个 worker 跑起来，从 cache 中获取的 worker 对 queue 来说是新的 worker
		return
	}

	// 没获取到 worker

	// Bail out early if it's in nonblocking mode or the number of pending callers reaches the maximum limit value.
	// 如果不能阻塞，或者阻塞的协程数已经到了设置的最大值
	if p.options.Nonblocking || (p.options.MaxBlockingTasks != 0 && p.Waiting() >= p.options.MaxBlockingTasks) {
		p.lock.Unlock()
		return nil, ErrPoolOverload
	}

	// Otherwise, we'll have to keep them blocked and wait for at least one worker to be put back into pool.
	p.addWaiting(1)
	p.cond.Wait() // block and wait for an available worker 等待被唤醒
	p.addWaiting(-1)

	if p.IsClosed() {
		p.lock.Unlock()
		return nil, ErrPoolClosed
	}

	goto retry
}

// revertWorker puts a worker back into free pool, recycling the goroutines.
func (p *Pool) revertWorker(worker *goWorker) bool {
	if capacity := p.Cap(); (capacity > 0 && p.Running() > capacity) || p.IsClosed() {
		p.cond.Broadcast() // 出错了，唤醒所有阻塞的协程，让它们检测到然后退出
		return false
	}

	worker.lastUsed = p.nowTime()

	// 对 worker 和为管理 worker 而设置的变量（running、waiting）的操作需要加锁
	// worker 中的方法本身都不是并发安全的

	p.lock.Lock()
	// To avoid memory leaks, add a double check in the lock scope.
	// Issue: https://github.com/panjf2000/ants/issues/113
	if p.IsClosed() { // TODO: 这里为什么要再检查一遍
		p.lock.Unlock()
		return false
	}
	if err := p.workers.insert(worker); err != nil { // 归还到队列
		p.lock.Unlock()
		return false
	}
	// Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	p.cond.Signal() // 唤醒一个等待 worker 的协程
	p.lock.Unlock()

	return true
}
