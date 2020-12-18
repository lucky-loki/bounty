package hunter

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lucky-loki/bounty"
)

// workerLoopQueue get worker with loopQueue strategy
type workerLoopQueue struct {
	workerCache sync.Pool

	lock       sync.Mutex
	workerList []Worker
	current    int // current worker index

	size                 int           // pool size
	workerExpiryInterval time.Duration // worker purge interval
	status               int64         // pool status
	stop                 chan struct{} // pool stop channel

	getSpent    Mean      // average time spent per Get() op by Milliseconds
	getInterval Mean      // interval between two Get() invoke by Milliseconds
	lastGetAt   time.Time // last time invoke Get()
	purgeSpent  Mean      // average time spent per purge() op by Milliseconds
}

// NewWorkerLoopQueue new a WorkerPool with loopQueue strategy
func NewWorkerLoopQueue(option WorkerPoolOption) WorkerPool {
	option.check()
	loopQueue := &workerLoopQueue{
		size:                 option.PoolSize,
		workerExpiryInterval: option.WorkerExpiryInterval,
		lastGetAt:            time.Now(),
		status:               closed,
	}

	loopQueue.workerCache.New = func() interface{} {
		return NewGoWorker(option.WorkerQueueSize)
	}
	return loopQueue
}

// Get return a least busy Worker as bounty.Consumer
func (wq *workerLoopQueue) Get() (bounty.Consumer, error) {
	begin := time.Now()

	wq.lock.Lock()
	defer wq.lock.Unlock()

	defer func() {
		delta := time.Since(begin).Milliseconds()
		wq.getSpent.Add(float64(delta))
	}()
	wq.getInterval.Add(float64(begin.Sub(wq.lastGetAt).Milliseconds()))
	wq.lastGetAt = begin

	start := wq.current
	var leastBusy Worker = &dummyWorker{}
	for {
		// get a worker if need spawn a new worker then spawn
		if len(wq.workerList) == wq.current {
			w := wq.workerCache.Get().(Worker)
			err := w.Run()
			if err != nil {
				return nil, err
			}
			wq.workerList = append(wq.workerList, w)
		}
		c := wq.workerList[wq.current]
		if c.BusyLevel() <= busyThreshold {
			leastBusy = c
			break
		}

		// store least busy worker and view next worker
		if c.BusyLevel() < leastBusy.BusyLevel() {
			leastBusy = c
		}
		wq.current++
		if wq.current == wq.size {
			wq.current = 0
		}

		// Back to the beginning, just break loop
		if wq.current == start {
			break
		}
	}

	if leastBusy.BusyLevel() >= busyLevelMax {
		defaultLogger.Printf("[WorkerLoopQueue] [Get] worker pool exhausted\n")
		return nil, errPoolExhausted
	}
	return leastBusy, nil
}

// Size return pool size
func (wq *workerLoopQueue) Size() int {
	return wq.size
}

func (wq *workerLoopQueue) Stats() *Stats {
	wq.lock.Lock()
	defer wq.lock.Unlock()

	s := Stats{
		MaxSize:     wq.size,
		CurrentSize: len(wq.workerList),
		Running:     wq.status == running,
		Get: MsPerOp{
			Ms:       wq.getSpent.Value(),
			N:        wq.getSpent.Count(),
			Interval: int64(wq.getInterval.Value()),
		},
		Purge: MsPerOp{
			Ms:       wq.purgeSpent.Value(),
			N:        wq.purgeSpent.Count(),
			Interval: wq.workerExpiryInterval.Milliseconds(),
		},
	}
	s.WorkerUsage = make([]BusyLevel, s.CurrentSize)
	for i, w := range wq.workerList {
		s.WorkerUsage[i] = w.BusyLevel()
	}

	return &s
}

type byBusyLevel []Worker

func (l byBusyLevel) Len() int {
	return len(l)
}

func (l byBusyLevel) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l byBusyLevel) Less(i, j int) bool {
	return l[i].BusyLevel() < l[j].BusyLevel()
}

func (wq *workerLoopQueue) purge() {
	begin := time.Now()
	// lock for forbid invoke Get() and Close()
	wq.lock.Lock()
	defer wq.lock.Unlock()
	defer func() {
		delta := time.Since(begin).Milliseconds()
		wq.purgeSpent.Add(float64(delta))
	}()

	if atomic.LoadInt64(&wq.status) == closed || len(wq.workerList) == 0 {
		return
	}
	defaultLogger.Printf("[WorkerLoopQueue] [Run] purge begin, pool_size: %d\n", len(wq.workerList))
	expiryTime := time.Now().Add(-wq.workerExpiryInterval)
	end := len(wq.workerList)
	for i := 0; i < end; i++ {
		w := wq.workerList[i]
		if w.IsEmpty() && w.LastActiveTime().Before(expiryTime) {
			err := w.Close()
			if err != nil {
				defaultLogger.Printf("[WorkerLoopQueue] [Run] purge error: %s\n", err)
			}
			// put back sync.Pool
			wq.workerCache.Put(w)
			// from tail search an active worker, then swap
			for end = end - 1; i < end; end-- {
				wt := wq.workerList[end]
				if wt.IsEmpty() && wt.LastActiveTime().Before(expiryTime) {
					err := wt.Close()
					if err != nil {
						defaultLogger.Printf("[WorkerLoopQueue] [Run] purge error: %s\n", err)
					}
					wq.workerCache.Put(wt)
					wq.workerList[end] = nil
					continue
				}
				break
			}
			// swap active to list head,
			wq.workerList[i], wq.workerList[end] = wq.workerList[end], nil
		}
	}
	wq.workerList = wq.workerList[:end]

	// sort fresh worker by BusyLevel
	sort.Sort(byBusyLevel(wq.workerList))
	wq.current = 0
	defaultLogger.Printf("[WorkerLoopQueue] [Run] purge done, pool_size: %d\n", len(wq.workerList))
}

// Run purge idle worker periodically
func (wq *workerLoopQueue) Run() {
	if !atomic.CompareAndSwapInt64(&wq.status, closed, running) {
		return
	}

	go func() {
		heartbeat := time.NewTicker(wq.workerExpiryInterval)
		defer heartbeat.Stop()

		for {
			select {
			case <-heartbeat.C:
				wq.purge()
			case <-wq.stop:
				defaultLogger.Printf("[WorkerLoopQueue] [Run] stop purge idle worker\n")
				return
			}
		}
	}()
}

// Close close all worker
func (wq *workerLoopQueue) Close() error {
	wq.lock.Lock()
	defer wq.lock.Unlock()

	defaultLogger.Printf("[workerLoopQueue] [Close] close pool\n")
	// loop for close worker
	var err error
	var errMsg []string
	for _, w := range wq.workerList {
		err = w.Close()
		if err != nil {
			errMsg = append(errMsg, fmt.Sprintf("[WorkerLoopQueue] close worker error: %s", err))
		}
	}
	wq.stop <- struct{}{}

	if len(errMsg) == 0 {
		return nil
	}
	msg := strings.Join(errMsg, "\n")
	defaultLogger.Printf("[workerLoopQueue] [Close] close pool occur error: %s\n", msg)
	return errors.New(msg)
}
