package hunter

import (
	"fmt"
	"testing"
	"time"

	"github.com/lucky-loki/bounty"
)

func simpleJob(sleep time.Duration) bounty.FuncJob {
	return func() {
		time.Sleep(sleep)
		fmt.Print("1 ")
	}
}

func TestGoWorker_Consume(t *testing.T) {
	worker := NewGoWorker(10)
	err := worker.Run()
	defer worker.Close()
	if err != nil {
		t.Errorf("worker run error: %s", err)
		return
	}
	errCount := 0
	for i := 0; i < 20; i++ {
		err = worker.Consume(simpleJob(10 * time.Millisecond))
		if err != nil {
			errCount++
		}
	}
	t.Logf("add job total fail count: %d", errCount)
	fmt.Println()
}
