package gmq

import "context"

const (
	TTLMsg     = 60 * 60 * 24 * 15 // 15 days
	TTLDeadMsg = 60 * 60 * 24 * 3  // 3 days
)

type Broker interface {
	Close() error
	Complete(ctx context.Context, msg IMsg) error
	Dequeue(ctx context.Context, queueName string) (*Msg, error)
	Delete(ctx context.Context, queueName, msgId string) error
	DeleteAgo(ctx context.Context, queueName string, seconds int64) error

	Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (*Msg, error)
	Fail(ctx context.Context, msg IMsg, errFail error) error
	Get(ctx context.Context, queueName, msgId string) (*Msg, error)
	GetStats(ctx context.Context) ([]*QueueStat, error)
	Init(ctx context.Context, queueName string) error

	Ping(ctx context.Context) error
	GetStatsByDate(ctx context.Context, YYYYMMDD string) (*QueueDailyStat, error)
	GetStatsWeekly(ctx context.Context) (*[]QueueDailyStat, *QueueDailyStat, error)
	Pause(ctx context.Context, Queuename string) error
	Resume(ctx context.Context, Queuename string) error
	// SetClock custom internal clock for testing
	SetClock(c Clock)
}
