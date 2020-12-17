Bounty
-------------

A High Performance CronJob and Routine Pool library


## Installation

```bash
go get -u github.com/lucky-loki/bounty
```

## Usage

### CronJob

```golang
package main

import (
  "fmt"
  "time"

  "github.com/lucky-loki/bounty"
  "github.com/lucky-loki/bounty/cron"
  "github.com/lucky-loki/bounty/hunter"
)

func printJob(format string, a ...interface{}) bounty.FuncJob {
  return func() {
    fmt.Printf(format, a...)
  }
}

func main() {
  cronJob := cron.NewCron(cron.NewMemeJobList(10000), hunter.NewHunter(hunter.WorkerPoolOption{PoolSize: 100}))
  cronJob.Run()
  
  specSche, err := cron.NewSpecSchedule("1-59 * * * * *", printJob("1\n"))
  if err != nil {
    panic(err.Error())
  }
  err = cronJob.Publish("printJob", specSche)
  if err != nil {
    panic(err.Error())
  }
  time.Sleep(15*time.Second)
}
```

### Routine Pool

```golang
package main

import (
  "encoding/json"
  "fmt"
  "time"

  "github.com/lucky-loki/bounty"
  "github.com/lucky-loki/bounty/hunter"
)

func simpleJob(sleep time.Duration) bounty.FuncJob {
  return func() {
    time.Sleep(sleep)
  }
}

func main() {
  var c bounty.Consumer = hunter.NewHunter(hunter.WorkerPoolOption{
    PoolSize:             10,
    WorkerQueueSize:      10,
    WorkerExpiryInterval: 5 * time.Minute,
  })
  for i := 0; i < 10; i++ {
    err := c.Consume(simpleJob(1 * time.Second))
    if err != nil {
      fmt.Printf("hunter consume error: %s\n", err)
    }
  }
  time.Sleep(15 * time.Second)
  
  // wait group usage
  wg, err := c.WaitGroup()
  if err != nil {
    fmt.Printf("fork a wait group error: %s", err)
    return
  }
  start := time.Now()
  var stats []byte
  for i := 0; i < 1000; i++ {
    if i%100 == 0 {
      // view pool stats
      stats, _ = json.Marshal(c.Stats())
      fmt.Printf("%s\n", stats)
    }
    // async first, resource is exhausted change to sync
    wg.MustConsume(simpleJob(1 * time.Second))
  }
  wg.Wait()
  timeSpent := time.Since(start)
  fmt.Printf("wait group consume job spent time: %s", timeSpent)
  stats, _ = json.Marshal(c.Stats())
  fmt.Printf("%s\n", stats)
}
```