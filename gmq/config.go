package gmq

type Config struct {
	Logger Logger

	QueueCfgs map[string]*QueueCfg
}
