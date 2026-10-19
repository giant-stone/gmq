package gmq

import (
	"context"
	"time"
)

const (
	// any state except failed message life time
	TTLMsg = 60 * 60 * 24 * 7 // 7 days

	// failed state message life time
	TTLDeadMsg = 60 * 60 * 24 * 3 // 3 days
)

const (
	DefaultTTLMsgUniq = time.Second * time.Duration(60*60*24) // 1 day
)

const (
	DefaultMaxItemsLimit = 10
)

type Broker interface {
	Close() error
	Complete(ctx context.Context, msg IMsg) error
	Dequeue(ctx context.Context, queueName string) (*Msg, error)
	DeleteMsg(ctx context.Context, queueName, id string) error
	DeleteQueue(ctx context.Context, queueName string) error
	DeleteAgo(ctx context.Context, queueName string, duration time.Duration) error

	Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (*Msg, error)
	Fail(ctx context.Context, msg IMsg, errFail error) error
	GetMsg(ctx context.Context, queueName, id string) (*Msg, error)

	// return a list message id(not internal msgId) of queue with specified name and limit
	ListMsg(ctx context.Context, queueName, state string, limit, offset int64) ([]string, error)

	// return a list failed message id of queue with specified queue name and message id
	// NOTICE: It is ordered from fresh to old.
	ListFailed(ctx context.Context, queueName, msgId string, limit, offset int64) ([]*Msg, error)

	ListQueue(ctx context.Context) ([]string, error)

	GetStats(ctx context.Context) ([]*QueueStat, error)
	Init(ctx context.Context, queueName string) error

	Ping(ctx context.Context) error
	GetStatsByDate(ctx context.Context, listQueueNames []string, YYYYMMDD string) (*QueueDailyStat, error)
	GetStatsWeekly(ctx context.Context, listQueueNames []string) ([]*QueueDailyStat, error)
	Pause(ctx context.Context, queueName string) error
	Resume(ctx context.Context, queueName string) error

	// SetClock custom internal clock for testing
	SetClock(c Clock)

	// processing time in UTC instead of local
	UTC(flag bool)
}

type QueueStat struct {
	Name       string `json:"name"`       // queue name
	Total      int64  `json:"total"`      // all state of message store in Redis
	Pending    int64  `json:"pending"`    // wait to free worker consume it
	Processing int64  `json:"processing"` // worker already took and consuming
	Failed     int64  `json:"failed"`     // occurred error, and/or pending to retry
}

type QueueDailyStat struct {
	Date      string `json:"date"` // YYYY-MM-DD in UTC
	Completed int64  `json:"completed"`
	Failed    int64  `json:"failed"`
	Total     int64  `json:"total"` // it is equal to Completed + Failed
}
