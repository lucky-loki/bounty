package hunter

type Stats struct {
	MaxSize     int         `json:"max_size"`
	CurrentSize int         `json:"current_size"`
	WorkerUsage []BusyLevel `json:"worker_usage"`
	Running     bool        `json:"running"`
	Get         MsPerOp     `json:"get"`
	Purge       MsPerOp     `json:"purge"`
}

type MsPerOp struct {
	Ms       float64 `json:"ms/op"`
	N        int     `json:"n"`
	Interval int64   `json:"interval(ms)"`
}

type Mean struct {
	mean float64
	n    int
}

func (m *Mean) Add(v float64) {
	m.n++
	m.mean = m.mean + (v-m.mean)/float64(m.n)
}

func (m *Mean) Value() float64 {
	return m.mean
}

func (m *Mean) Count() int {
	return m.n
}
