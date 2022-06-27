package gmq

type QueueCfg struct {
	opts []OptionServer
}

func NewQueueCfg(opts ...OptionServer) *QueueCfg {
	return &QueueCfg{opts: opts}
}
