package cron

import (
	"fmt"
	"testing"
	"time"

	"github.com/lucky-loki/bounty"
	"github.com/lucky-loki/bounty/hunter"
)

func printJob(format string, a ...interface{}) bounty.FuncJob {
	return func() {
		fmt.Printf(format, a...)
	}
}

func TestNewCron(t *testing.T) {
	cronJob := NewCron(NewMemeJobList(100), hunter.NewHunter(hunter.WorkerPoolOption{PoolSize: 100}))
	cronJob.Run()
	err := cronJob.PublishIntervalJob("just print", 2*time.Second, printJob("1\n"))
	if err != nil {
		t.Logf("publish job error: %s", err)
		return
	}
	time.Sleep(20 * time.Second)
}

func TestCron_ExecAt(t *testing.T) {
	cronJob := NewCron(NewMemeJobList(100), hunter.NewHunter(hunter.WorkerPoolOption{PoolSize: 100}))
	cronJob.Run()
	past := time.Now().Add(-time.Minute)
	err := cronJob.ExecAt("run in the past", past, printJob("run in the past\n"))
	if err != nil {
		t.Logf("run job in the past error: %s", err)
		return
	}
	future := time.Now().Add(30 * time.Second)
	err = cronJob.ExecAt("run in the future", future, printJob("run in the future\n"))
	if err != nil {
		t.Logf("run job in the past error: %s", err)
		return
	}
	time.Sleep(3 * time.Minute)
}

func TestNewSpecSchedule(t *testing.T) {
	cronJob := NewCron(NewMemeJobList(100), hunter.NewHunter(hunter.WorkerPoolOption{PoolSize: 100}))
	cronJob.Run()
	specSche, err := NewSpecSchedule("1-59 * * * * *", printJob("1\n"))
	if err != nil {
		t.Error(err)
		return
	}
	err = cronJob.Publish("just print", specSche)
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(15*time.Second)
}
