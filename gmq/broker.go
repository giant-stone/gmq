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
	Failed     int64  `json:"failed"`     // occured error, and/or pending to retry
}

type QueueDailyStat struct {
	Date      string `json:"date" gorm:"primarykey"` // YYYY-MM-DD in UTC
	Completed int64  `json:"completed"`
	Failed    int64  `json:"failed"`
	Total     int64  `json:"total"` // it is equal to Completed + Failed
}

// BrokerUnimplemented must be embedded to have forward compatible implementations.
type BrokerUnimplemented struct{}

// Close implements Broker
func (*BrokerUnimplemented) Close() error {
	return ErrNotImplemented
}

// Complete implements Broker
func (*BrokerUnimplemented) Complete(ctx context.Context, msg IMsg) error {
	return ErrNotImplemented
}

// DeleteAgo implements Broker
func (*BrokerUnimplemented) DeleteAgo(ctx context.Context, queueName string, seconds int64) error {
	return ErrNotImplemented
}

// DeleteMsg implements Broker
func (*BrokerUnimplemented) DeleteMsg(ctx context.Context, queueName string, msgId string) error {
	return ErrNotImplemented
}

// DeleteQueue implements Broker
func (*BrokerUnimplemented) DeleteQueue(ctx context.Context, queueName string) error {
	return ErrNotImplemented
}

// Dequeue implements Broker
func (*BrokerUnimplemented) Dequeue(ctx context.Context, queueName string) (*Msg, error) {
	return nil, ErrNotImplemented
}

// Enqueue implements Broker
func (*BrokerUnimplemented) Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (*Msg, error) {
	return nil, ErrNotImplemented
}

// Fail implements Broker
func (*BrokerUnimplemented) Fail(ctx context.Context, msg IMsg, errFail error) error {
	return ErrNotImplemented
}

// GetMsg implements Broker
func (*BrokerUnimplemented) GetMsg(ctx context.Context, queueName string, msgId string) (*Msg, error) {
	return nil, ErrNotImplemented
}

// GetStats implements Broker
func (*BrokerUnimplemented) GetStats(ctx context.Context) ([]*QueueStat, error) {
	return nil, ErrNotImplemented
}

// GetStatsByDate implements Broker
func (*BrokerUnimplemented) GetStatsByDate(ctx context.Context, YYYYMMDD string) (*QueueDailyStat, error) {
	return nil, ErrNotImplemented
}

// GetStatsWeekly implements Broker
func (*BrokerUnimplemented) GetStatsWeekly(ctx context.Context) (*[]QueueDailyStat, *QueueDailyStat, error) {
	return nil, nil, ErrNotImplemented
}

// Init implements Broker
func (*BrokerUnimplemented) Init(ctx context.Context, queueName string) error {
	return ErrNotImplemented
}

// ListMsg implements Broker
func (*BrokerUnimplemented) ListMsg(ctx context.Context, queueName string, state string, limit int64, offset int64) ([]string, error) {
	return nil, ErrNotImplemented
}

// Pause implements Broker
func (*BrokerUnimplemented) Pause(ctx context.Context, queueName string) error {
	return ErrNotImplemented
}

// Ping implements Broker
func (*BrokerUnimplemented) Ping(ctx context.Context) error {
	return ErrNotImplemented
}

// Resume implements Broker
func (*BrokerUnimplemented) Resume(ctx context.Context, queueName string) error {
	return ErrNotImplemented
}

// SetClock implements Broker
func (*BrokerUnimplemented) SetClock(c Clock) {}
