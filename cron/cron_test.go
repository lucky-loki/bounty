package cron

import (
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"testing"
	"time"

	"github.com/lucky-loki/bounty"
	"github.com/lucky-loki/orm"
)

type printExec struct {}

func (p *printExec) Run(v []byte) {
	fmt.Println(string(v))
}


type execMapLibrary map[string]bounty.Executable

func (el execMapLibrary) Get(path string) (bounty.Executable, error) {
	e, ok := el[path]
	if !ok {
		return nil, errors.New("not found")
	}
	return e, nil
}

var execLibrary = execMapLibrary{
	"/print": new(printExec),
}

func connect() (*gorm.DB, error) {
	c := db.Config{
		Host: "localhost",
		Port: 3306,
		User: "root",
		Password: "sun1990",
		Database: "god",
	}
	return c.OpenMysql()
}

func TestCron_Publish_Schedule(t *testing.T) {
	db_, err := connect()
	if err != nil {
		t.Error(err)
		return
	}
	cronJob := Default("myCronJob", execLibrary, db_)
	cronJob.Run()
	err = cronJob.Publish(&SpecJob{
		JobName: "print_1",
		ExecutablePath: "/print",
		Argv: []byte("1"),
		Spec: "1-59 * * * * *",
	})
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(1*time.Minute)
}

func TestCron_Publish_OneTime(t *testing.T) {
	db_, err := connect()
	if err != nil {
		t.Error(err)
		return
	}
	cronJob := Default("myCronJob", execLibrary, db_)
	cronJob.Run()
	err = cronJob.Publish(&SpecJob{
		JobName: "print_one_time",
		ExecutablePath: "/print",
		Argv: []byte("1"),
		OneTime: true,
		Next: time.Now().Add(10*time.Second).Unix(),
	})
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(1*time.Minute)
}


func TestCron_Load(t *testing.T) {
	db_, err := connect()
	if err != nil {
		t.Error(err)
		return
	}
	cronJob := Default("myCronJob", execLibrary, db_)
	err = cronJob.Load()
	cronJob.Run()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(1*time.Minute)
}

func TestCron_Load_OneTime(t *testing.T) {
	db_, err := connect()
	if err != nil {
		t.Error(err)
		return
	}
	cronJob := Default("myCronJob", execLibrary, db_)
	cronJob.Run()
	err = cronJob.Publish(&SpecJob{
		JobName: "print_one_time",
		ExecutablePath: "/print",
		Argv: []byte("1"),
		OneTime: true,
		Next: time.Now().Add(1*time.Minute).Unix(),
	})
	if err != nil {
		t.Error(err)
		return
	}
	// mock crash
	cronJob.Stop()

	// mock reboot then load and
	cronJob2 := Default("myCronJob", execLibrary, db_)
	err = cronJob2.Load()
	if err != nil {
		t.Error(err)
		return
	}
	cronJob2.Run()
	time.Sleep(2*time.Minute)

}