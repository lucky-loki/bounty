package cron

import (
	"errors"
	"github.com/jinzhu/gorm"
	"github.com/lucky-loki/bounty"
	"math"
	"time"
)

const (
	specJobTable = "spec_job"
	yearByMinute = 365 * 24 * 60 * time.Minute
)

type mysqlJobList struct {
	execLibrary ExecutableLibrary
	cronName string
	db *gorm.DB
	meme *memeJobList
}

func newMysqlJobList(cronName string, db *gorm.DB, execLibrary ExecutableLibrary) JobList {
	m := &mysqlJobList{
		execLibrary: execLibrary,
		cronName:    cronName,
		db:          db,
		meme:        newMemeJobList(math.MaxInt64),
	}
	if err := db.AutoMigrate(new(SpecJob)).Error; err != nil {
		panic("mysql job list table create error: "+err.Error())
	}
	return m
}

// Load query belongs to this cronName's cron job,
// and new a SpecSchedule add to job list to run
func (mj *mysqlJobList) Load() error {
	//  query exist cron job
	var list []*SpecJob
	err := mj.db.Where("cron_name=?", mj.cronName).Find(&list).Error
	if err != nil {
		return err
	}

	// restore to meme
	now := time.Now()
	var s SpecJob
	var sche Schedule
	for _, j := range list {
		// remove one time schedule job, because program crash unable remove normally
		if j.OneTime && time.Unix(j.Next, 0).Before(now) {
			err = mj.db.Unscoped().Where("cron_name=? and job_name=?", mj.cronName, j.JobName).Delete(&s).Error
			if err != nil {
				err = errors.New("remove already executed ome time schedule job error: " + err.Error())
				return err
			}
			continue
		}

		// new schedule job
		if j.OneTime {
			j.Spec = ""
			sche, err = mj.newConstantDelaySchedule(20*yearByMinute, j.ExecutablePath, j.Argv)
		} else {
			sche, err = mj.newSpecScheduleJob(j.Spec, j.ExecutablePath, j.Argv)
		}
		if err != nil {
			return err
		}

		// add job to memeJobList to schedule
		err = mj.meme.Add(j.JobName, sche, j.OneTime, time.Unix(j.Next, 0))
		if err != nil {
			err = errors.New("add schedule job to memeJobList error: " + err.Error())
			return err
		}
	}
	return nil
}

func (mj *mysqlJobList) newSpecScheduleJob(spec, path string, argv []byte) (Schedule, error) {
	// query execLibrary
	exec, err := mj.execLibrary.Get(path)
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

func (mj *mysqlJobList) newConstantDelaySchedule(duration time.Duration, path string, argv []byte) (Schedule, error) {
	// query execLibrary
	exec, err := mj.execLibrary.Get(path)
	if err != nil {
		err = errors.New("find executable job error: " + err.Error())
		return nil, err
	}

	// new a spec schedule
	job := func() {exec.Run(argv)}
	sche := Every(duration, bounty.FuncJob(job))
	return sche, nil
}

func (mj *mysqlJobList) Add(sj *SpecJob) error {
	_, err := mj.meme.Query(sj.JobName)
	if err == nil {
		return errJobAlreadyExist
	}
	var sche Schedule
	if sj.OneTime {
		sj.Spec = ""
		sche, err = mj.newConstantDelaySchedule(20*yearByMinute, sj.ExecutablePath, sj.Argv)
	} else {
		sche, err = mj.newSpecScheduleJob(sj.Spec, sj.ExecutablePath, sj.Argv)
	}
	if err != nil {
		return err
	}
	// one time job's Next already set to future time
	if !sj.OneTime {
		sj.Next = sche.Next(time.Now()).Unix()
	}
	sj.Prev = 0
	sj.CronName = mj.cronName
	err = mj.db.Create(sj).Error
	if err != nil {
		return err
	}
	return mj.meme.Add(sj.JobName, sche, sj.OneTime, time.Unix(sj.Next, 0))
}

func (mj *mysqlJobList) QueryExpireSoonest() (*Entry, error) {
	return mj.meme.QueryExpireSoonest()
}

func (mj *mysqlJobList) Query(name string) (bounty.Job, error) {
	return mj.meme.Query(name)
}

func (mj *mysqlJobList) Update(name string, now time.Time) error {
	sche, err := mj.meme.Query(name)
	if err != nil {
		return err
	}
	// update mysql
	columns := map[string]int64{
		"prev": now.Unix(),
		"next": sche.Next(now).Unix(),
	}
	err = mj.db.Table(specJobTable).Where(
		"`cron_name`=? and `job_name`=?", mj.cronName, name).Updates(columns).Error
	if err != nil {
		return err
	}
	// update meme
	return mj.meme.Update(name, now)
}

func (mj *mysqlJobList) Remove(name string) (*Entry, error) {
	_, err := mj.meme.Query(name)
	if err != nil {
		return nil, err
	}
	var s SpecJob
	err = mj.db.Unscoped().Where("cron_name=? and job_name=?", mj.cronName, name).Delete(&s).Error
	if err != nil {
		return nil, err
	}
	return mj.meme.Remove(name)
}
