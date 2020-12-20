package cron

import (
	"errors"
	"github.com/lucky-loki/bounty"
	"math"
	"time"
)

const (
	yearByMinute = 365 * 24 * 60 * time.Minute
)

// The Schedule describe a cycle job
type Schedule interface {
	Next(time.Time) time.Time
	bounty.Job
}

type JobListPersistence interface {
	Add(sj *SpecJob) error
	ListAll(cronName string) ([]*SpecJob, error)
	Update(cronName, jobName string, prev, next int64) error
	Remove(cronName, jobName string) error
}

var _ JobListPersistence = &mysqlJobListPersistence{}

type JobList struct {
	cronName    string
	execLibrary ExecutableLibrary
	persistence JobListPersistence
	meme        *memeJobList
}

func newJobList(cronName string, execLibrary ExecutableLibrary, persistence JobListPersistence) *JobList {
	m := &JobList{
		execLibrary: execLibrary,
		cronName:    cronName,
		persistence: persistence,
		meme:        newMemeJobList(math.MaxInt64),
	}
	return m
}

// Load query from persistence and load into memeJobList
func (jl *JobList) Load() error {
	//  query exist cron job
	list, err := jl.persistence.ListAll(jl.cronName)
	if err != nil {
		return err
	}

	// restore to meme
	now := time.Now()
	for _, job := range list {
		// remove one time schedule job, because program crash unable remove normally
		if job.OneTime && time.Unix(job.Next, 0).Before(now) {
			err = jl.persistence.Remove(job.CronName, job.JobName)
			if err != nil {
				err = errors.New("remove already executed ome time schedule job error: " + err.Error())
				return err
			}
			continue
		}

		// new schedule job
		if job.OneTime {
			job.Spec = ""
			job.Schedule, err = jl.newConstantDelaySchedule(20*yearByMinute, job.ExecutablePath, job.Argv)
		} else {
			job.Schedule, err = jl.newSpecScheduleJob(job.Spec, job.ExecutablePath, job.Argv)
		}
		if err != nil {
			return err
		}

		// add job to memeJobList to schedule
		err = jl.meme.Add(job)
		if err != nil {
			err = errors.New("add schedule job to memeJobList error: " + err.Error())
			return err
		}
	}
	return nil
}

func (jl *JobList) newSpecScheduleJob(spec, path string, argv []byte) (Schedule, error) {
	// query execLibrary
	exec, err := jl.execLibrary.Get(path)
	if err != nil {
		err = errors.New("find executable job error: " + err.Error())
		return nil, err
	}

	// new a spec schedule
	var sche Schedule
	job := func() {exec.Run(argv)}
	sche, err = NewSpecSchedule(spec, bounty.FuncJob(job))
	if err != nil {
		err = errors.New("new spec schedule error: " + err.Error())
		return nil, err
	}
	return sche, nil
}

func (jl *JobList) newConstantDelaySchedule(duration time.Duration, path string, argv []byte) (Schedule, error) {
	// query execLibrary
	exec, err := jl.execLibrary.Get(path)
	if err != nil {
		err = errors.New("find executable job error: " + err.Error())
		return nil, err
	}

	// new a spec schedule
	job := func() {exec.Run(argv)}
	sche := Every(duration, bounty.FuncJob(job))
	return sche, nil
}

func (jl *JobList) Add(job *SpecJob) error {
	_, err := jl.meme.Query(job.JobName)
	if err == nil {
		return errJobAlreadyExist
	}
	// parse schedule
	if job.OneTime {
		job.Spec = ""
		job.Schedule, err = jl.newConstantDelaySchedule(20*yearByMinute, job.ExecutablePath, job.Argv)
	} else {
		job.Schedule, err = jl.newSpecScheduleJob(job.Spec, job.ExecutablePath, job.Argv)
	}
	if err != nil {
		return err
	}
	// one time job's Next already set to future time
	if !job.OneTime {
		job.Next = job.Schedule.Next(time.Now()).Unix()
	}
	job.Prev = 0
	job.CronName = jl.cronName
	err = jl.persistence.Add(job)
	if err != nil {
		return err
	}
	return jl.meme.Add(job)
}

func (jl *JobList) QueryExpireSoonest() (*Entry, error) {
	return jl.meme.QueryExpireSoonest()
}

func (jl *JobList) Query(name string) (bounty.Job, error) {
	return jl.meme.Query(name)
}

func (jl *JobList) Update(name string, now time.Time) error {
	sche, err := jl.meme.Query(name)
	if err != nil {
		return err
	}
	// update persistence layer
	prev, next := now.Unix(), sche.Next(now).Unix()
	err = jl.persistence.Update(jl.cronName, name, prev, next)
	if err != nil {
		defaultLogger.Printf("[Run] [JobList] [Update] persistence update error: %s\n", err)
	}
	// update meme
	return jl.meme.Update(name, prev, next)
}

func (jl *JobList) Remove(name string) (*Entry, error) {
	_, err := jl.meme.Query(name)
	if err != nil {
		return nil, err
	}
	err = jl.persistence.Remove(jl.cronName, name)
	if err != nil {
		return nil, err
	}
	return jl.meme.Remove(name)
}