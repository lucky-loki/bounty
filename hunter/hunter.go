package hunter

import (
	"errors"
	"sync"
	"time"

	"github.com/lucky-loki/bounty"
)

var (
	errWorkerQueueFull  = errors.New("worker's queue is full")
	errWorkerNotRunning = errors.New("worker not running, you should invoke Run() first")
	errJobNil           = errors.New("job can not be nil")
	errPoolExhausted    = errors.New("pool almost exhausted")
)

const (
	defaultWorkerPoolSize       = 10000
	defaultWorkerExpiryInterval = 5 * time.Minute
	defaultWorkerQueueSize      = 100

	closed = iota
	running
)

// WorkerPool can get a worker from pool
type WorkerPool interface {
	// Get
	Get() (bounty.Consumer, error)
	Size() int
	Stats() *Stats
	Run()
	Close() error
}

// Worker extends bounty.Consumer interface
// and support Run and Close method
type Worker interface {
	bounty.Consumer
	BusyLevel() BusyLevel
	LastActiveTime() time.Time
	IsEmpty() bool
	Run() error
	Close() error
}

var _ WorkerPool = &workerLoopQueue{}
var _ Worker = &goWorker{}
var _ bounty.Consumer = &Hunter{}

// Hunter provide async process
type Hunter struct {
	pool WorkerPool
}

// NewHunter new hunter with a loop queue worker pool
func NewHunter(option WorkerPoolOption) *Hunter {
	h := &Hunter{
		pool: NewWorkerLoopQueue(option),
	}
	h.pool.Run()
	return h
}

// Consume job will be Run as async method, if pool's resource is exhausted,
// will return an error
func (h *Hunter) Consume(job bounty.Job) error {
	w, err := h.pool.Get()
	if err != nil {
		defaultLogger.Printf("[Consume] get worker from pool error: %s", err)
		return err
	}
	return w.Consume(job)
}

// WaitGroup wrapper for sync.WaitGroup with pool to async job
func (h *Hunter) WaitGroup() (*WaitGroup, error) {
	wg := &WaitGroup{
		pool: h.pool,
		wg:   sync.WaitGroup{},
	}
	return wg, nil
}

func (h *Hunter) Stats() *Stats {
	return h.pool.Stats()
}

// Close close worker pool
func (h *Hunter) Close() error {
	return h.pool.Close()
}

// WorkerPoolOption option for worker pool
type WorkerPoolOption struct {
	PoolSize             int
	WorkerQueueSize      int
	WorkerExpiryInterval time.Duration
}

func (o *WorkerPoolOption) check() {
	if o.PoolSize <= 0 {
		o.PoolSize = defaultWorkerPoolSize
	}
	if o.WorkerExpiryInterval <= 0 {
		o.WorkerExpiryInterval = defaultWorkerExpiryInterval
	}
	if o.WorkerQueueSize <= 0 {
		o.WorkerQueueSize = defaultWorkerQueueSize
	}
}
