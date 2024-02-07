package ants

import "time"

// TODO: 这里面的操作都没有上锁，但是并发访问的问题是存在的，怎么解决，锁上在了哪里

// 基于数组实现的循环队列
type loopQueue struct {
	items  []worker
	expiry []worker // 过期的 worker
	head   int
	tail   int
	size   int
	isFull bool
}

func newWorkerLoopQueue(size int) *loopQueue {
	return &loopQueue{
		items: make([]worker, size),
		size:  size,
	}
}

func (wq *loopQueue) len() int {
	if wq.size == 0 || wq.isEmpty() {
		return 0
	}

	if wq.head == wq.tail && wq.isFull {
		return wq.size
	}

	if wq.tail > wq.head {
		return wq.tail - wq.head
	}

	return wq.size - wq.head + wq.tail
}

func (wq *loopQueue) isEmpty() bool {
	return wq.head == wq.tail && !wq.isFull
}

func (wq *loopQueue) insert(w worker) error {
	if wq.size == 0 {
		return errQueueIsReleased
	}

	if wq.isFull {
		return errQueueIsFull
	}
	wq.items[wq.tail] = w
	wq.tail = (wq.tail + 1) % wq.size

	if wq.tail == wq.head {
		wq.isFull = true
	}

	return nil
}

func (wq *loopQueue) detach() worker {
	if wq.isEmpty() {
		return nil
	}

	w := wq.items[wq.head]
	wq.items[wq.head] = nil
	wq.head = (wq.head + 1) % wq.size

	wq.isFull = false

	return w
}

func (wq *loopQueue) refresh(duration time.Duration) []worker {
	expiryTime := time.Now().Add(-duration) // worker 中标记 lastUsed 上次使用时间，应该大于这个 expiryTime，小于等于它的全部应该被清理
	// lastUsed 的更新时机是 worker 执行完一个任务被放回 workerQueue 的时候
	// 所以这个过期时间指的是从执行完上一个任务到现在这个时间点的空窗期（没有任务可执行，一直阻塞在 run 处）
	// queue 中的 worker 都是当前没有任务可执行，阻塞在 run 处的，需要定时被清理
	index := wq.binarySearch(expiryTime) // workerQueue 是按 lastUsed 时间有序的
	if index == -1 {
		return nil
	}
	wq.expiry = wq.expiry[:0] // 这种写法是复用了底层的 slice 对象和其内部的数组指针

	if wq.head <= index {
		wq.expiry = append(wq.expiry, wq.items[wq.head:index+1]...)
		for i := wq.head; i < index+1; i++ {
			wq.items[i] = nil
		}
	} else {
		wq.expiry = append(wq.expiry, wq.items[0:index+1]...)
		wq.expiry = append(wq.expiry, wq.items[wq.head:]...)
		for i := 0; i < index+1; i++ {
			wq.items[i] = nil
		}
		for i := wq.head; i < wq.size; i++ {
			wq.items[i] = nil
		}
	}
	head := (index + 1) % wq.size
	wq.head = head
	if len(wq.expiry) > 0 {
		wq.isFull = false
	}

	return wq.expiry
}

func (wq *loopQueue) binarySearch(expiryTime time.Time) int {
	var mid, nlen, basel, tmid int
	nlen = len(wq.items)

	// if no need to remove work, return -1
	if wq.isEmpty() || expiryTime.Before(wq.items[wq.head].lastUsedTime()) {
		return -1
	}

	// example
	// size = 8, head = 7, tail = 4
	// [ 2, 3, 4, 5, nil, nil, nil,  1]  true position
	//   0  1  2  3    4   5     6   7
	//              tail          head
	//
	//   1  2  3  4  nil nil   nil   0   mapped position
	//            r                  l

	// base algorithm is a copy from worker_stack
	// map head and tail to effective left and right
	r := (wq.tail - 1 - wq.head + nlen) % nlen
	basel = wq.head
	l := 0
	for l <= r {
		mid = l + ((r - l) >> 1) // avoid overflow when computing mid
		// calculate true mid position from mapped mid position
		tmid = (mid + basel + nlen) % nlen
		if expiryTime.Before(wq.items[tmid].lastUsedTime()) {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	// return true position from mapped position
	return (r + basel + nlen) % nlen
}

func (wq *loopQueue) reset() {
	if wq.isEmpty() {
		return
	}

retry:
	if w := wq.detach(); w != nil {
		w.finish()
		goto retry
	}
	wq.items = wq.items[:0]
	wq.size = 0
	wq.head = 0
	wq.tail = 0
}
