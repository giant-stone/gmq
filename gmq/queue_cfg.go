package gmq

type QueueCfg struct {
	opts []OptionQueue
}

func NewQueueCfg(opts ...OptionQueue) *QueueCfg {
	return &QueueCfg{opts: opts}
}
