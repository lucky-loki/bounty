package cron

import (
	"github.com/lucky-loki/bounty"
	"time"
)

type SpecJob struct {
	CronName string		`gorm:"unique_index:uuid"`

	JobName string		`gorm:"unique_index:uuid"`
	ExecutablePath string
	Argv []byte

	Spec string
	OneTime bool	// just run job one time, then will remove the job

	Prev int64
	Next int64
}

// The Schedule describe a cycle job
type Schedule interface {
	Next(time.Time) time.Time
	bounty.Job
}

type JobList interface {
	Load() error
	Add(sj *SpecJob) error
	QueryExpireSoonest() (*Entry, error)
	Query(name string) (bounty.Job, error)
	Update(name string, now time.Time) error
	Remove(name string) (*Entry, error)
}

var _ JobList = &mysqlJobList{}