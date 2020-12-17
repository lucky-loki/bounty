package hunter

import (
	"time"

	"github.com/lucky-loki/bounty"
)

// BusyLevel mark worker busy level, value between [0,100)
// 0 represent very free, 100 represent very busy.
type BusyLevel int

const (
	busyThreshold BusyLevel = 25
	busyLevelMax  BusyLevel = 90
)

func busyLevel(value int, cap int) BusyLevel {
	lv := BusyLevel(value * 100 / cap)
	if lv > busyLevelMax {
		lv = busyLevelMax
	}
	return lv
}

type dummyWorker struct{}

func (b *dummyWorker) BusyLevel() BusyLevel         { return busyLevelMax + 100 }
func (b *dummyWorker) Consume(job bounty.Job) error { return errWorkerQueueFull }
func (b *dummyWorker) LastActiveTime() time.Time    { return time.Time{} }
func (b *dummyWorker) IsEmpty() bool                { return false }
func (b *dummyWorker) Run() error                   { return nil }
func (b *dummyWorker) Close() error                 { return nil }
