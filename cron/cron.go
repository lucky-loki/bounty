package cron

import (
	"errors"
	"github.com/jinzhu/gorm"
	"github.com/lucky-loki/bounty/hunter"
	"sync/atomic"
	"time"

	"github.com/lucky-loki/bounty"
)

const (
	stopped = iota
	running
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries can
// be removed while running.
type Cron struct {
	Name string

	consumer bounty.Consumer
	execLibrary ExecutableLibrary
	jobList  *JobList

	running int64
	newJob  chan struct{} // notify blocked schedule routine
	stop    chan struct{} // stop schedule routine
}

type ExecutableLibrary interface {
	Get(path string) (bounty.Executable, error)
}

type SpecJob struct {
	CronName       string		`gorm:"unique_index:uuid"`
	JobName        string		`gorm:"unique_index:uuid"`
	ExecutablePath string
	Argv           []byte
	Spec           string
	OneTime        bool	// just run job one time, then will remove the job
	Prev           int64
	Next           int64
	Schedule       Schedule `gorm:"-"`
}

// Entry the schedule unit
type Entry struct {
	Schedule Schedule
	Name     string // unique for each schedule job
	Index    int
	Prev     time.Time
	Next     time.Time
	OneTime  bool
}

// NewCron return a cron instance with memeJobList + mysql persistence layer
func Default(cronName string, execLibrary ExecutableLibrary, db *gorm.DB) *Cron {
	c := &Cron{
		consumer: hunter.NewHunter(hunter.WorkerPoolOption{}),
		jobList:  newJobList(cronName, execLibrary, newMysqlPersistence(db)),
		running:  stopped,
		newJob:   make(chan struct{}, 1),
		stop:     make(chan struct{}, 1),
	}
	return c
}

func NewCron(cronName string, execLibrary ExecutableLibrary, persistence JobListPersistence) *Cron {
	c := &Cron{
		consumer: hunter.NewHunter(hunter.WorkerPoolOption{}),
		jobList:  newJobList(cronName, execLibrary, persistence),
		running:  stopped,
		newJob:   make(chan struct{}, 1),
		stop:     make(chan struct{}, 1),
	}
	return c
}

// Load query from persistence and load into memeJobList
func (c *Cron) Load() error {
	return c.jobList.Load()
}

// Publish a new Schedule job
func (c *Cron) Publish(specJob *SpecJob) error {
	if specJob == nil {
		return errors.New("specJob can not be nil")
	}
	defaultLogger.Printf("[Publish] add new job (%s)\n", specJob.JobName)
	err := c.jobList.Add(specJob)
	if err != nil {
		defaultLogger.Printf("[Publish] new job (%s) add to cron job error: %s\n", specJob.JobName, err)
		return err
	}

	select {
	case c.newJob <- struct{}{}:
	default:
	}
	return nil
}

// Exec job by manual and sync method, this will not update job next exec time
func (c *Cron) Exec(name string) error {
	defer bounty.WithRecover()

	job, err := c.jobList.Query(name)
	if err != nil {
		return err
	}
	job.Run()
	return nil
}

// RemoveJob remove job from cron by jobId
func (c *Cron) RemoveJob(name string) (*Entry, error) {
	defaultLogger.Printf("[RemoveJob] remove job (%s)\n", name)
	e, err := c.jobList.Remove(name)
	if err != nil {
		defaultLogger.Printf("[RemoveJob] job (%s) remove error: %s\n", name, err)
	}
	return e, err
}

// Run loop query latest expire job and push it to consumer.
// if you Run a already running cron will be a no-op
func (c *Cron) Run() {
	ok := atomic.CompareAndSwapInt64(&c.running, stopped, running)
	if !ok {
		return
	}

	go func() {
		var timer *time.Timer
		var latest *Entry
		var now time.Time
		for {
			latest = c.peek()
			if latest == nil {
				timer = time.NewTimer(5 * time.Minute)
			} else {
				timer = time.NewTimer(latest.Next.Sub(time.Now()))
			}
			select {
			case now = <-timer.C:
				latest = c.peek()
				if latest == nil || latest.Next.After(now) {
					break
				}
				c.consume(latest, now)
			case <-c.newJob:
				defaultLogger.Println("[Run] new job notify")
				timer.Stop()
			case <-c.stop:
				defaultLogger.Println("[Run] schedule routine exit")
				timer.Stop()
				return
			}
		}
	}()
}

// Stop stop cron, not clear job list.
// if you stop a not running cron will be a no-op
func (c *Cron) Stop() {
	ok := atomic.CompareAndSwapInt64(&c.running, running, stopped)
	if !ok {
		return
	}

	select {
	case c.stop <- struct{}{}:
	default:
	}
}

func (c *Cron) peek() *Entry {
	defer bounty.WithRecover()

	latest, err := c.jobList.QueryExpireSoonest()
	if err != nil {
		defaultLogger.Printf("query expire soonest job error: %s\n", err)
	}
	return latest
}

func (c *Cron) update(e *Entry, now time.Time) {
	defer bounty.WithRecover()

	err := c.jobList.Update(e.Name, now)
	if err != nil {
		defaultLogger.Printf("[Run] update job (%s) next time error: %s\n", e.Name, err)
	}
}

func (c *Cron) consume(e *Entry, now time.Time) {
	defer bounty.WithRecover()
	defer c.update(e, now)

	job := func() {
		e.Schedule.Run()
		if e.OneTime {
			_, _ = c.RemoveJob(e.Name)
		}
	}

	// run schedule job
	defaultLogger.Printf("[Run] job (%s) consumed prev: %s\n", e.Name, e.Prev)
	err := c.consumer.Consume(bounty.FuncJob(job))
	if err != nil {
		defaultLogger.Printf("[Run] job (%s) consumed error: %s\n", e.Name, err)
		defaultLogger.Printf("[Run] job (%s) run by sync method\n", e.Name)
		// run schedule by sync method
		job()
	}
}
