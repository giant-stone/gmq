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
	listWaiting    map[string]*list.List
	listProcessing map[string]*list.List
	listCompleted  map[string]*list.List
	listFailed     map[string]*list.List

	msgIdUniq map[string]struct{}
	msgDetail map[string]*Msg

	maxBytes  int64
	namespace string

	queuesPaused map[string]struct{}
}

type BrokerInMemoryOpts struct {
	MaxBytes int64
}

// Close implements Broker
func (it *BrokerInMemory) Close() error {
	it.lock.Lock()
	defer it.lock.Unlock()

	it.listPending = nil
	it.listWaiting = nil
	it.listProcessing = nil
	it.listCompleted = nil
	it.listFailed = nil

	it.msgIdUniq = nil
	it.msgDetail = nil

	it.queuesPaused = nil

	return nil
}

// Complete implements Broker
func (it *BrokerInMemory) Complete(ctx context.Context, msg IMsg) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	msgId := NewKeyMsgDetail(it.namespace, msg.GetQueue(), msg.GetId())
	rawMsg, ok := it.msgDetail[msgId]
	if !ok {
		return ErrNoMsg
	}

	rawMsg.State = MsgStateCompleted

	now := it.clock.Now()
	nowInMs := now.UnixMilli()
	rawMsg.Updated = nowInMs

	return nil
}

// DeleteAgo implements Broker
func (it *BrokerInMemory) DeleteAgo(ctx context.Context, queueName string, duration time.Duration) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	now := it.clock.Now()
	nowInMs := now.UnixMilli()

	type pair struct {
		NewKeyQueueFunc func(_namespace, _queueName string) string
		L               map[string]*list.List
	}

	for _, t := range []pair{
		{
			NewKeyQueuePending,
			it.listPending,
		},

		{
			NewKeyQueueWaiting,
			it.listWaiting,
		},

		{
			NewKeyQueueProcessing,
			it.listProcessing,
		},

		{
			NewKeyQueueCompleted,
			it.listCompleted,
		},

		{
			NewKeyQueueFailed,
			it.listFailed,
		},
	} {
		key := t.NewKeyQueueFunc(it.namespace, queueName)
		if l, ok := t.L[key]; ok {
			for e := l.Front(); e != nil; e = e.Next() {
				msgId := e.Value.(string)
				rawMsg := it.msgDetail[msgId]
				if rawMsg.Expiredat > 0 && rawMsg.Expiredat > nowInMs {
					continue
				} else if rawMsg.Expiredat == 0 {
					expiredat := time.UnixMilli(rawMsg.Created).Add(duration).UnixMilli()
					if expiredat > nowInMs {
						continue
					}
				}

				removeElementFromList(l, msgId)
				delete(it.msgIdUniq, msgId)
				delete(it.msgDetail, msgId)
			}
		}
	}

	return nil
}

// DeleteMsg implements Broker
func (it *BrokerInMemory) DeleteMsg(ctx context.Context, queueName string, id string) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	msgId := NewKeyMsgDetail(it.namespace, queueName, id)

	key := NewKeyQueuePending(it.namespace, queueName)
	if l, ok := it.listPending[key]; ok {
		removeElementFromList(l, msgId)
	}

	key = NewKeyQueueWaiting(it.namespace, queueName)
	if l, ok := it.listWaiting[key]; ok {
		removeElementFromList(l, msgId)
	}

	key = NewKeyQueueProcessing(it.namespace, queueName)
	if l, ok := it.listProcessing[key]; ok {
		removeElementFromList(l, msgId)
	}

	key = NewKeyQueueCompleted(it.namespace, queueName)
	if l, ok := it.listCompleted[key]; ok {
		removeElementFromList(l, msgId)
	}

	key = NewKeyQueueFailed(it.namespace, queueName)
	if l, ok := it.listFailed[key]; ok {
		removeElementFromList(l, msgId)
	}

	delete(it.msgIdUniq, msgId)
	delete(it.msgDetail, msgId)

	return nil
}

func removeElementFromList(l *list.List, b string) {
	for e := l.Front(); e != nil; e = e.Next() {
		if a, ok := e.Value.(string); ok && a == b {
			l.Remove(e)
			break
		}
	}
}

// DeleteQueue implements Broker
func (it *BrokerInMemory) DeleteQueue(ctx context.Context, queueName string) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	type pair struct {
		NewKeyQueueFunc func(_namespace, _queueName string) string
		L               map[string]*list.List
	}

	for _, t := range []pair{
		{
			NewKeyQueuePending,
			it.listPending,
		},

		{
			NewKeyQueueWaiting,
			it.listWaiting,
		},

		{
			NewKeyQueueProcessing,
			it.listProcessing,
		},

		{
			NewKeyQueueCompleted,
			it.listCompleted,
		},

		{
			NewKeyQueueFailed,
			it.listFailed,
		},
	} {
		key := t.NewKeyQueueFunc(it.namespace, queueName)
		if l, ok := t.L[key]; ok {
			for e := l.Front(); e != nil; e = e.Next() {
				msgId := e.Value.(string)

				removeElementFromList(l, msgId)
				delete(it.msgIdUniq, msgId)
				delete(it.msgDetail, msgId)

				delete(t.L, key)
			}
		}
	}

	return nil
}

// Dequeue implements Broker
func (it *BrokerInMemory) Dequeue(ctx context.Context, queueName string) (*Msg, error) {
	it.lock.Lock()
	defer it.lock.Unlock()

	keyQueueName := NewKeyQueuePaused(it.namespace, queueName)
	if _, ok := it.queuesPaused[keyQueueName]; ok {
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
	rawMsg.Updated = nowInMs
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

	if _, dup := it.msgIdUniq[msgId]; dup {
		return nil, ErrMsgIdConflict
	}

	if msg, ok := it.msgDetail[msgId]; ok {
		if msg.Expiredat > nowInMs {
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
		Updated:   nowInMs,
	}
	it.msgIdUniq[msgId] = struct{}{}
	it.msgDetail[msgId] = rawMsg

	return rawMsg, nil
}

// Fail implements Broker
func (it *BrokerInMemory) Fail(ctx context.Context, msg IMsg, errFail error) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	msgId := NewKeyMsgDetail(it.namespace, msg.GetQueue(), msg.GetId())
	rawMsg, ok := it.msgDetail[msgId]
	if !ok {
		return ErrNoMsg
	}

	rawMsg.Err = errFail.Error()
	rawMsg.State = MsgStateFailed
	now := it.clock.Now()
	nowInMs := now.UnixMilli()
	rawMsg.Updated = nowInMs

	return nil
}

// GetMsg implements Broker
func (it *BrokerInMemory) GetMsg(ctx context.Context, queueName string, id string) (*Msg, error) {
	it.lock.RLock()
	defer it.lock.RUnlock()

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
	it.lock.RLock()
	defer it.lock.RUnlock()

	rs := make([]string, 0)

	var l *list.List
	var ok bool
	switch state {
	case MsgStatePending:
		{
			l, ok = it.listPending[queueName]
		}
	case MsgStateWaiting:
		{
			l, ok = it.listWaiting[queueName]
		}
	case MsgStateProcessing:
		{
			l, ok = it.listProcessing[queueName]
		}
	case MsgStateCompleted:
		{
			l, ok = it.listCompleted[queueName]
		}
	case MsgStateFailed:
		{
			l, ok = it.listFailed[queueName]
		}
	}

	if !ok {
		return rs, nil
	}

	o := 0
	n := 0
	for e := l.Front(); e != nil; e = e.Next() {
		if o < int(offset) {
			o += 1
			continue
		}
		msgId := e.Value.(string)
		rs = append(rs, msgId)
		n += 1
	}

	return rs, nil
}

// Pause implements Broker
func (it *BrokerInMemory) Pause(ctx context.Context, queueName string) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	it.queuesPaused[queueName] = struct{}{}
	return nil
}

// Ping implements Broker
func (it *BrokerInMemory) Ping(ctx context.Context) error {
	return nil
}

// Resume implements Broker
func (it *BrokerInMemory) Resume(ctx context.Context, queueName string) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	delete(it.queuesPaused, queueName)
	return nil
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
		lock:           sync.RWMutex{},
		listPending:    make(map[string]*list.List),
		listProcessing: make(map[string]*list.List),
		listCompleted:  make(map[string]*list.List),
		listFailed:     make(map[string]*list.List),
		queuesPaused:   make(map[string]struct{}),
		msgIdUniq:      make(map[string]struct{}),
		msgDetail:      make(map[string]*Msg),
		maxBytes:       DefaultMaxBytes,
		namespace:      DefaultQueueName,
	}
	if opts != nil {
		broker.maxBytes = opts.MaxBytes
	}
	return broker, nil
}
