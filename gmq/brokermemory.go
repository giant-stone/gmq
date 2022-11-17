package gmq

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultMaxBytes = 1024 * 1024 * 32 // 32 MB
)

type BrokerInMemory struct {
	BrokerUnimplemented

	clock Clock
	lock  sync.RWMutex

	listPending    map[string]*list.List
	listProcessing map[string]*list.List
	listPause      map[string]struct{}

	msgDetail map[string]*Msg

	maxBytes  int64
	namespace string
}

type BrokerInMemoryOpts struct {
	MaxBytes int64
}

// Close implements Broker
func (it *BrokerInMemory) Close() error {
	it.lock.Lock()
	defer it.lock.Unlock()

	it.listPending = nil
	it.listProcessing = nil
	it.listPause = nil
	it.msgDetail = nil

	return nil
}

// Complete implements Broker
func (it *BrokerInMemory) Complete(ctx context.Context, msg IMsg) error {
	panic("unimplemented")
}

// DeleteAgo implements Broker
func (it *BrokerInMemory) DeleteAgo(ctx context.Context, queueName string, seconds int64) error {
	panic("unimplemented")
}

// DeleteMsg implements Broker
func (it *BrokerInMemory) DeleteMsg(ctx context.Context, queueName string, id string) error {
	panic("unimplemented")
}

// DeleteQueue implements Broker
func (it *BrokerInMemory) DeleteQueue(ctx context.Context, queueName string) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	for msgId := range it.listPending {
		delete(it.msgDetail, msgId)
	}
	key := NewKeyQueuePending(it.namespace, queueName)
	delete(it.listPending, key)

	for msgId := range it.listProcessing {
		delete(it.msgDetail, msgId)
	}
	key = NewKeyQueueProcessing(it.namespace, queueName)
	delete(it.listProcessing, key)

	return nil
}

// Dequeue implements Broker
func (it *BrokerInMemory) Dequeue(ctx context.Context, queueName string) (*Msg, error) {
	it.lock.Lock()
	defer it.lock.Unlock()

	keyQueueName := NewKeyQueuePaused(it.namespace, queueName)
	if _, ok := it.listPause[keyQueueName]; ok {
		return nil, ErrNoMsg
	}

	keyQueueName = NewKeyQueuePending(it.namespace, queueName)
	listPending, ok := it.listPending[keyQueueName]
	if !ok {
		return nil, ErrNoMsg
	}

	e := listPending.Front()
	if e == nil {
		return nil, ErrNoMsg
	}

	msgId := e.Value.(string)
	listPending.Remove(e)

	keyQueueName = NewKeyQueueProcessing(it.namespace, queueName)
	if listProcessing, ok := it.listProcessing[keyQueueName]; !ok {
		return nil, ErrNoMsg
	} else {
		listProcessing.PushBack(msgId)
	}

	rawMsg := it.msgDetail[msgId]

	now := it.clock.Now()
	nowInMs := now.UnixMilli()
	rawMsg.Processedat = nowInMs
	rawMsg.State = MsgStateProcessing
	return rawMsg, nil
}

// Enqueue implements Broker
func (it *BrokerInMemory) Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (*Msg, error) {
	it.lock.Lock()
	defer it.lock.Unlock()

	payload := msg.GetPayload()
	id := msg.GetId()
	if id == "" {
		id = uuid.NewString()
	}

	queueName := msg.GetQueue()

	var uniqueInMs int64
	for _, opt := range opts {
		switch opt.Type() {
		case OptTypeQueueName:
			{
				value := opt.Value().(string)
				if value != "" {
					queueName = value
				}
			}
		case OptTypeUniqueIn:
			{
				value := int64(opt.Value().(time.Duration).Milliseconds())
				if value > 0 {
					uniqueInMs = value
				}
			}
		}
	}

	if queueName == "" {
		queueName = DefaultQueueName
	}

	it.updateQueueList(ctx, queueName)

	now := it.clock.Now()
	nowInMs := now.UnixMilli()
	msgId := NewKeyMsgDetail(it.namespace, queueName, id)

	var expiredAt int64
	if uniqueInMs == 0 {
		expiredAt = 0
	} else {
		expiredAt = now.Add(time.Millisecond * time.Duration(nowInMs)).UnixMilli()
	}

	if old, dup := it.msgDetail[msgId]; dup {
		if old.Expiredat == 0 || old.Expiredat > nowInMs {
			return nil, ErrMsgIdConflict
		}
	}

	keyQueueName := NewKeyQueuePending(it.namespace, queueName)
	l := it.listPending[keyQueueName]
	l.PushBack(msgId)

	rawMsg := &Msg{
		Created:   nowInMs,
		Expiredat: expiredAt,
		Id:        id,
		Payload:   payload,
		Queue:     queueName,
		State:     MsgStatePending,
	}
	it.msgDetail[msgId] = rawMsg

	return rawMsg, nil
}

// Fail implements Broker
func (it *BrokerInMemory) Fail(ctx context.Context, msg IMsg, errFail error) error {
	panic("unimplemented")
}

// GetMsg implements Broker
func (it *BrokerInMemory) GetMsg(ctx context.Context, queueName string, id string) (*Msg, error) {
	it.lock.Lock()
	defer it.lock.Unlock()

	msgId := NewKeyMsgDetail(it.namespace, queueName, id)
	if msg, ok := it.msgDetail[msgId]; !ok {
		return nil, ErrNoMsg
	} else {
		return msg, nil
	}
}

// GetStats implements Broker
func (it *BrokerInMemory) GetStats(ctx context.Context) ([]*QueueStat, error) {
	panic("unimplemented")
}

// GetStatsByDate implements Broker
func (it *BrokerInMemory) GetStatsByDate(ctx context.Context, YYYYMMDD string) (*QueueDailyStat, error) {
	panic("unimplemented")
}

// GetStatsWeekly implements Broker
func (it *BrokerInMemory) GetStatsWeekly(ctx context.Context) (*[]QueueDailyStat, *QueueDailyStat, error) {
	panic("unimplemented")
}

// Init implements Broker
func (it *BrokerInMemory) Init(ctx context.Context, queueName string) error {
	it.lock.Lock()
	defer it.lock.Unlock()
	return it.updateQueueList(ctx, queueName)
}

// ListMsg implements Broker
func (it *BrokerInMemory) ListMsg(ctx context.Context, queueName string, state string, limit int64, offset int64) ([]string, error) {
	panic("unimplemented")
}

// Pause implements Broker
func (it *BrokerInMemory) Pause(ctx context.Context, Queuename string) error {
	panic("unimplemented")
}

// Ping implements Broker
func (it *BrokerInMemory) Ping(ctx context.Context) error {
	return nil
}

// Resume implements Broker
func (it *BrokerInMemory) Resume(ctx context.Context, Queuename string) error {
	panic("unimplemented")
}

// SetClock implements Broker
func (it *BrokerInMemory) SetClock(c Clock) {
	it.clock = c
}

func (it *BrokerInMemory) updateQueueList(ctx context.Context, queueName string) (err error) {
	key := NewKeyQueuePending(it.namespace, queueName)
	if _, ok := it.listPending[key]; !ok {
		it.listPending[key] = list.New()
	}

	key = NewKeyQueueProcessing(it.namespace, queueName)
	if _, ok := it.listProcessing[key]; !ok {
		it.listProcessing[key] = list.New()
	}
	return nil
}

func NewBrokerInMemory(opts *BrokerInMemoryOpts) (rs Broker, err error) {
	broker := &BrokerInMemory{
		clock:          NewWallClock(),
		listPending:    make(map[string]*list.List),
		listProcessing: make(map[string]*list.List),
		listPause:      make(map[string]struct{}),
		msgDetail:      make(map[string]*Msg),
		maxBytes:       DefaultMaxBytes,
		lock:           sync.RWMutex{},
	}
	if opts != nil {
		broker.maxBytes = opts.MaxBytes
	}
	return broker, nil
}
