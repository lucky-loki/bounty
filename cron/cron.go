package cron

import (
	"errors"
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
	consumer bounty.Consumer
	jobList  JobList

	running int64
	newJob  chan struct{} // notify blocked schedule routine
	stop    chan struct{} // stop schedule routine
}

// The Schedule describe a cycle job
type Schedule interface {
	Next(time.Time) time.Time
	bounty.Job
}

type JobList interface {
	Add(name string, schedule Schedule) error
	QueryExpireSoonest() (*Entry, error)
	Query(name string) (bounty.Job, error)
	Update(name string, now time.Time) error
	Remove(name string) (*Entry, error)
	Reset() error
}

var _ JobList = &MemeJobList{}

// Entry the schedule unit
type Entry struct {
	Schedule Schedule
	Name     string // unique for each schedule job
	Index    int
	Prev     time.Time
	Next     time.Time
}

// NewCron return a inited cron instance
func NewCron(jobList JobList, consumer bounty.Consumer) *Cron {
	c := &Cron{
		consumer: consumer,
		jobList:  jobList,
		running:  stopped,
		newJob:   make(chan struct{}, 1),
		stop:     make(chan struct{}, 1),
	}
	return c
}

// ExecAt run a job in future then remove it
func (c *Cron) ExecAt(name string, future time.Time, s bounty.Job) error {
	delta := future.Sub(time.Now())
	runThenRemove := func() {
		s.Run()
		_, _ = c.RemoveJob(name)
	}
	return c.PublishIntervalFunc(name, delta, runThenRemove)
}

// Publish a new Schedule job
func (c *Cron) Publish(name string, s Schedule) error {
	if s == nil {
		return errors.New("schedule can not be nil")
	}
	defaultLogger.Printf("[Publish] add new job (%s)\n", name)
	err := c.jobList.Add(name, s)
	if err != nil {
		defaultLogger.Printf("[Publish] new job (%s) add to cron job error: %s\n", name, err)
		return err
	}

	select {
	case c.newJob <- struct{}{}:
	default:
	}
	return nil
}

// PublishIntervalFunc publish a func cycle executed with interval
func (c *Cron) PublishIntervalFunc(name string, interval time.Duration, f func()) error {
	return c.PublishIntervalJob(name, interval, bounty.FuncJob(f))
}

// PublishIntervalJob publish a job cycle executed with interval
func (c *Cron) PublishIntervalJob(name string, interval time.Duration, job bounty.Job) error {
	s := Every(interval, job)
	return c.Publish(name, s)
}

// Exec job by manual and sync method
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

func (c *Cron) ClearJobList() error {
	return c.jobList.Reset()
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

	defaultLogger.Printf("[Run] job (%s) consumed prev: %s\n", e.Name, e.Prev)
	err := c.consumer.Consume(e.Schedule)
	if err != nil {
		defaultLogger.Printf("[Run] job (%s) consumed error: %s\n", e.Name, err)
		// run schedule by sync method
		defaultLogger.Printf("[Run] job (%s) run by sync method\n", e.Name)
		e.Schedule.Run()
	}
}
