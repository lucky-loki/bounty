package cron

import (
	"container/heap"
	"errors"
	"sync"
	"time"
)

var (
	errCronJobListFull = errors.New("cron's job list is full")
	errJobAlreadyExist = errors.New("job name already exist")
	errJobNotFound     = errors.New("job not found")

	defaultCronSize = 100000 // 默认支持十万个CronJob
)

type memeJobList struct {
	sync.Mutex
	jobList jobHeap
	jobMap  map[string]*Entry
	maxSize int
}

func newMemeJobList(size int) *memeJobList {
	m := &memeJobList{
		jobList: make([]*Entry, 0),
		jobMap:  make(map[string]*Entry),
		maxSize: defaultCronSize,
	}
	if size > 0 {
		m.maxSize = size
	}
	return m
}

func (m *memeJobList) Add(job *SpecJob) error {
	m.Lock()
	defer m.Unlock()

	if len(m.jobList) >= m.maxSize {
		return errCronJobListFull
	}
	_, ok := m.jobMap[job.JobName]
	if ok {
		return errJobAlreadyExist
	}

	e := &Entry{Schedule: job.Schedule, Name: job.JobName, Next: time.Unix(job.Next, 0), OneTime: job.OneTime}
	heap.Push(&m.jobList, e)
	m.jobMap[e.Name] = e

	return nil
}

func (m *memeJobList) QueryExpireSoonest() (*Entry, error) {
	m.Lock()
	defer m.Unlock()
	e := m.jobList.Peek()
	if e == nil {
		return nil, nil
	}
	eCopy := *e
	return &eCopy, nil
}

func (m *memeJobList) Query(name string) (Schedule, error)  {
	m.Lock()
	defer m.Unlock()

	e, ok := m.jobMap[name]
	if !ok {
		return nil, errJobNotFound
	}
	return e.Schedule, nil
}

func (m *memeJobList) Update(name string, prev, next int64) error {
	m.Lock()
	defer m.Unlock()

	e, ok := m.jobMap[name]
	if !ok {
		return errJobNotFound
	}
	e.Prev = time.Unix(prev, 0)
	e.Next = time.Unix(next, 0)
	heap.Fix(&m.jobList, e.Index)

	return nil
}

func (m *memeJobList) Remove(name string) (*Entry, error) {
	m.Lock()
	defer m.Unlock()

	e, ok := m.jobMap[name]
	if !ok {
		return nil, errJobNotFound
	}
	delete(m.jobMap, name)
	heap.Remove(&m.jobList, e.Index)
	return e, nil
}

// jobHeap implement job list by heap for fast query
// and add a new job with sorted `Next` property
type jobHeap []*Entry

func (jh jobHeap) Len() int {
	return len(jh)
}

func (jh jobHeap) Less(i, j int) bool {
	return jh[i].Next.Before(jh[j].Next)
}

func (jh jobHeap) Swap(i, j int) {
	jh[i], jh[j] = jh[j], jh[i]
	jh[i].Index = i
	jh[j].Index = j
}

// Push append a Entry to list tail
func (jh *jobHeap) Push(x interface{}) {
	e := x.(*Entry)
	e.Index = len(*jh)
	*jh = append(*jh, e)
}

// Pop pop a Entry from list tail
func (jh *jobHeap) Pop() interface{} {
	old := *jh
	n := len(old)
	if n == 0 {
		return nil
	}

	e := old[n-1]
	e.Index = -1
	old[n-1] = nil
	*jh = old[0 : n-1]
	return e
}

func (jh *jobHeap) Peek() *Entry {
	if len(*jh) == 0 {
		return nil
	}
	return (*jh)[0]
}
