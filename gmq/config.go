package gmq

import "time"

const (
	DefaultDurationRestIfNoMsg = time.Second * time.Duration(1)
)

type Config struct {
	Logger Logger

	MsgMaxTTL   time.Duration
	RestIfNoMsg time.Duration

	QueueCfgs map[string]*QueueCfg
}
