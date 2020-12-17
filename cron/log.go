package cron

import (
	"log"
	"os"
)

var defaultLogger = Logger(log.New(os.Stderr, "[CronJob] ", log.LstdFlags|log.Lmsgprefix))

type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
