package gmq

import "time"

type Config struct {
	Logger Logger

	MsgMaxTTL   time.Duration
	RestIfNoMsg time.Duration

	QueueCfgs map[string]*QueueCfg
}
