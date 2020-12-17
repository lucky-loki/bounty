package hunter

import (
	"sync/atomic"
	"time"

	"github.com/lucky-loki/bounty"
)

type goWorker struct {
	taskQueue chan bounty.Job
	jobCount  int64
	size      int
	status    int64
	active    time.Time // last active time
}

// NewGoWorker return a go routine worker
func NewGoWorker(size int) Worker {
	if size <= 0 {
		size = defaultWorkerQueueSize
	}
	w := &goWorker{
		size:   size,
		status: closed,
	}
	return w
}

// Consume a job and push to taskQueue, if queue full will return an error.
// you should ensure worker is Running and job not be nil
func (w *goWorker) Consume(job bounty.Job) (err error) {
	if atomic.LoadInt64(&w.status) == closed {
		return errWorkerNotRunning
	}
	if job == nil {
		return errJobNil
	}
	defer func() {
		if recover() != nil {
			err = errWorkerNotRunning
		}
	}()

	select {
	case w.taskQueue <- job:
		atomic.AddInt64(&w.jobCount, 1)
		return nil
	default:
		return errWorkerQueueFull
	}
}

// runWithRecover wrapper job invoke with recover function
func runWithRecover(job bounty.Job) {
	defer bounty.WithRecover()
	job.Run()
}

// Run change status to running, init a taskQueue and start a routine
// work on it. if you Run() a Running worker, this will be a no-op
func (w *goWorker) Run() error {
	if !atomic.CompareAndSwapInt64(&w.status, closed, running) {
		return nil
	}

	w.taskQueue = make(chan bounty.Job, w.size)
	go func() {
		for job := range w.taskQueue {
			runWithRecover(job)
			w.active = time.Now()
			atomic.AddInt64(&w.jobCount, -1)
		}
	}()
	return nil
}

// Close close taskQueue and not accept new job.
// if you Close() a Closed worker, this will be a no-op
func (w *goWorker) Close() error {
	if !atomic.CompareAndSwapInt64(&w.status, running, closed) {
		return nil
	}

	close(w.taskQueue)
	return nil
}

func (w *goWorker) BusyLevel() BusyLevel {
	return busyLevel(int(w.jobCount), w.size)
}

func (w *goWorker) LastActiveTime() time.Time {
	return w.active
}

func (w *goWorker) IsEmpty() bool {
	return w.jobCount == 0
}
