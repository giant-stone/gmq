package gmq

import (
	"context"
	"fmt"
	"time"
)

// BrokerUnimplemented supplies defaults for custom brokers that support only a
// subset of Broker. Embed it by value and override the operations your application
// uses. Unsupported operations return an error wrapping ErrNotImplemented.
// Close, SetClock, and UTC do nothing; override them when resources or time
// configuration need to be managed. Embedding does not guarantee compatibility
// with future changes to the Broker interface.
type BrokerUnimplemented struct{}

var _ Broker = BrokerUnimplemented{}

// Close does nothing because the helper owns no resources.
func (BrokerUnimplemented) Close() error { return nil }

// SetClock does nothing because the helper performs no time-dependent work.
func (BrokerUnimplemented) SetClock(Clock) {}

// UTC does nothing because the helper performs no time-dependent work.
func (BrokerUnimplemented) UTC(bool) {}

// Complete returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) Complete(ctx context.Context, msg IMsg) error {
	return fmt.Errorf("Complete: %w", ErrNotImplemented)
}

// Dequeue returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) Dequeue(ctx context.Context, queueName string) (*Msg, error) {
	return nil, fmt.Errorf("Dequeue: %w", ErrNotImplemented)
}

// DeleteMsg returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) DeleteMsg(ctx context.Context, queueName, id string) error {
	return fmt.Errorf("DeleteMsg: %w", ErrNotImplemented)
}

// DeleteQueue returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) DeleteQueue(ctx context.Context, queueName string) error {
	return fmt.Errorf("DeleteQueue: %w", ErrNotImplemented)
}

// DeleteAgo returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) DeleteAgo(ctx context.Context, queueName string, duration time.Duration) error {
	return fmt.Errorf("DeleteAgo: %w", ErrNotImplemented)
}

// Enqueue returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (*Msg, error) {
	return nil, fmt.Errorf("Enqueue: %w", ErrNotImplemented)
}

// Fail returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) Fail(ctx context.Context, msg IMsg, errFail error) error {
	return fmt.Errorf("Fail: %w", ErrNotImplemented)
}

// GetMsg returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) GetMsg(ctx context.Context, queueName, id string) (*Msg, error) {
	return nil, fmt.Errorf("GetMsg: %w", ErrNotImplemented)
}

// ListMsg returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) ListMsg(ctx context.Context, queueName, state string, limit, offset int64) ([]string, error) {
	return nil, fmt.Errorf("ListMsg: %w", ErrNotImplemented)
}

// ListFailed returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) ListFailed(ctx context.Context, queueName, msgId string, limit, offset int64) ([]*Msg, error) {
	return nil, fmt.Errorf("ListFailed: %w", ErrNotImplemented)
}

// ListQueue returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) ListQueue(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("ListQueue: %w", ErrNotImplemented)
}

// GetStats returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) GetStats(ctx context.Context) ([]*QueueStat, error) {
	return nil, fmt.Errorf("GetStats: %w", ErrNotImplemented)
}

// Init returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) Init(ctx context.Context, queueName string) error {
	return fmt.Errorf("Init: %w", ErrNotImplemented)
}

// Ping returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) Ping(ctx context.Context) error {
	return fmt.Errorf("Ping: %w", ErrNotImplemented)
}

// GetStatsByDate returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) GetStatsByDate(ctx context.Context, queueNames []string, date string) (*QueueDailyStat, error) {
	return nil, fmt.Errorf("GetStatsByDate: %w", ErrNotImplemented)
}

// GetStatsWeekly returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) GetStatsWeekly(ctx context.Context, queueNames []string) ([]*QueueDailyStat, error) {
	return nil, fmt.Errorf("GetStatsWeekly: %w", ErrNotImplemented)
}

// Pause returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) Pause(ctx context.Context, queueName string) error {
	return fmt.Errorf("Pause: %w", ErrNotImplemented)
}

// Resume returns an error wrapping ErrNotImplemented.
func (BrokerUnimplemented) Resume(ctx context.Context, queueName string) error {
	return fmt.Errorf("Resume: %w", ErrNotImplemented)
}
