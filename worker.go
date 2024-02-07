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
	"runtime/debug"
	"time"
)

// goWorker is the actual executor who runs the tasks,
// it starts a goroutine that accepts tasks and
// performs function calls.
// worker 用于创建并长时间运行一个不回收的协程，这个协程不断处理用户提交的任务
// 等到该 worker 的 task 通道中没有要处理的任务时，该协程就会被回收
type goWorker struct {
	// pool who owns this worker.
	pool *Pool // worker 需要反向和 pool 绑定，因为 worker 一石被获取就会从队列中被摘下来

	// task is a job should be done.
	task chan func() // 要处理的任务

	// lastUsed will be updated when putting a worker back into queue.
	lastUsed time.Time // 最后一次被使用完的时间
}

// run starts a goroutine to repeat the process
// that performs the function calls.
func (w *goWorker) run() {
	w.pool.addRunning(1)
	go func() {
		defer func() { // 如果一个 worker 走到这个 defer 了，其实就是被清理了
			w.pool.addRunning(-1)
			w.pool.workerCache.Put(w)     // 被清理的 worker 归还到 workerCache，由 sync.Pool 来管理
			if p := recover(); p != nil { // 捕获 panic
				if ph := w.pool.options.PanicHandler; ph != nil {
					ph(p)
				} else {
					w.pool.options.Logger.Printf("worker exits from panic: %v\n%s\n", p, debug.Stack())
				}
			}
			// Call Signal() here in case there are goroutines waiting for available workers.
			w.pool.cond.Signal() // 唤醒一个阻塞在 retrieveWorker 的协程
			// 如果没有这个，假设 workerQueue 中的 worker 全部被清理了，那所有阻塞在 retrieveWorker 的协程就都会一直阻塞了
			// 但是其实是可以向 workerCache 申请的
			// 所以这里要多唤醒一次
		}()

		for f := range w.task { // 如果这个 worker 又被获取到了，这里就会读到新任务，否则会阻塞，直到这个 worker 被清理
			if f == nil {
				return
			}
			f()
			if ok := w.pool.revertWorker(w); !ok { // 归还到 queue
				return
			} // 将 worker 归还到 workerQueue，并唤醒一个协程
		}
	}()
}

func (w *goWorker) finish() {
	w.task <- nil
}

func (w *goWorker) lastUsedTime() time.Time {
	return w.lastUsed
}

func (w *goWorker) inputFunc(fn func()) {
	w.task <- fn
}

func (w *goWorker) inputParam(interface{}) {
	panic("unreachable")
}
