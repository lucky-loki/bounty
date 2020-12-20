package cron

import "github.com/jinzhu/gorm"

const (
	specJobTable = "spec_job"
)

type mysqlJobListPersistence struct {
	db *gorm.DB
}

func newMysqlPersistence(db *gorm.DB) JobListPersistence {
	m := &mysqlJobListPersistence{
		db: db,
	}
	if err := db.AutoMigrate(new(SpecJob)).Error; err != nil {
		panic("mysql job list table create error: "+err.Error())
	}
	return m
}

// ListAll query all job belongs to this cronName
func (mj *mysqlJobListPersistence) ListAll(cronName string) (list []*SpecJob, err error) {
	err = mj.db.Where("cron_name=?", cronName).Find(&list).Error
	return
}

func (mj *mysqlJobListPersistence) Add(sj *SpecJob) (err error) {
	err = mj.db.Create(sj).Error
	return
}

func (mj *mysqlJobListPersistence) Update(cronName, jobName string, prev, next int64) error {
	columns := map[string]int64{
		"prev": prev,
		"next": next,
	}
	err := mj.db.Table(specJobTable).Where(
		"`cron_name`=? and `job_name`=?", cronName, jobName).Updates(columns).Error
	return err
}

func (mj *mysqlJobListPersistence) Remove(cronName, jobName string) error {
	var s SpecJob
	err := mj.db.Unscoped().Where("cron_name=? and job_name=?", cronName, jobName).Delete(&s).Error
	return err
}
