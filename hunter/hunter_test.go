package hunter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lucky-loki/bounty"
)

func TestHunter_Consume(t *testing.T) {
	var c bounty.Consumer = NewHunter(WorkerPoolOption{
		PoolSize:             10,
		WorkerQueueSize:      10,
		WorkerExpiryInterval: 5 * time.Minute,
	})
	for i := 0; i < 10; i++ {
		err := c.Consume(simpleJob(1 * time.Second))
		if err != nil {
			t.Logf("hunter consume error: %s\n", err)
		}
	}
	time.Sleep(15 * time.Second)
}

func TestHunter_WaitGroup(t *testing.T) {
	var c = NewHunter(WorkerPoolOption{
		PoolSize:             10,
		WorkerQueueSize:      10,
		WorkerExpiryInterval: 1 * time.Second,
	})
	wg, err := c.WaitGroup()
	if err != nil {
		t.Logf("fork a wait group error: %s", err)
		return
	}
	start := time.Now()
	var stats []byte
	for i := 0; i < 100; i++ {
		if i%100 == 0 {
			stats, _ = json.Marshal(c.Stats())
			t.Logf("%s\n", stats)
		}
		wg.MustConsume(simpleJob(1 * time.Second))
	}
	wg.Wait()
	timeSpent := time.Since(start)
	t.Logf("wait group consume job spent time: %s", timeSpent)
	stats, _ = json.Marshal(c.Stats())
	t.Logf("%s\n", stats)
}
