package bounty

import (
	"log"
	"runtime"
)

// A Publisher can be used to publish a job
type Publisher interface {
	Publish(job Job)
}

// A Consumer accept a job and run it by async method
type Consumer interface {
	// Consume accept a job and run it by async method.
	//
	// if Consumer's resource is exhausted, must refuse
	// the job and return an error
	Consume(job Job) error
}

// A Job is a interface for publish and cunsume
type Job interface {
	Run()
}

type Executable interface {
	Run(v []byte)
}

// FuncJob warpper a func() into a Job
type FuncJob func()

// Run function
func (f FuncJob) Run() { f() }

func WithRecover()  {
	if p := recover(); p != nil {
		var buf [4096]byte
		n := runtime.Stack(buf[:], false)
		log.Printf("[goWorker] job exits from panic: %s\n", string(buf[:n]))
	}
}