package hunter

import (
	"sync"

	"github.com/lucky-loki/bounty"
)

// A WaitGroup must not be copied after first use.
type WaitGroup struct {
	pool WorkerPool
	wg   sync.WaitGroup
}

func jobWithWg(wg *sync.WaitGroup, job bounty.Job) bounty.FuncJob {
	wg.Add(1)
	return func() {
		defer wg.Done()
		job.Run()
	}
}

// MustConsume ensure job will be done, if pool's resource is exhausted
// job will run as sync method
func (wq *WaitGroup) MustConsume(job bounty.Job) {
	if job == nil {
		return
	}
	jobNew := jobWithWg(&wq.wg, job)
	w, err := wq.pool.Get()
	if err == nil {
		err = w.Consume(jobNew)
		if err == nil {
			return
		}
	}
	defaultLogger.Printf("[WaitGroup] [MustConsume] push job to queue: %s", err)
	jobNew.Run()
}

// Wait will be blocked until all job done
func (wq *WaitGroup) Wait() {
	wq.wg.Wait()
}
