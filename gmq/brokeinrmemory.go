package gmq

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/giant-stone/go/gtime"
	"github.com/google/uuid"
)

const (
	DefaultMaxBytes = 1024 * 1024 * 32 // 32 MB
)

type MsgHistory map[string]*list.List // key is <msgId>, value is <msg>

type BrokerInMemory struct {
	BrokerUnimplemented

	clock Clock

	failedHistory map[string]MsgHistory // key is <queueName>

	utc  bool
	lock sync.RWMutex

	listPending    map[string]*list.List // key is <queueName>, list.Element value is <messageId>
	listProcessing map[string]*list.List
	listFailed     map[string]*list.List

	listStat map[string]*list.List // key is <queueName>, list.Element value is *QueueDailyStat

	msgDetail map[string]map[string]*Msg  // key is <queueName>, value is <msgId> => <msg>
	msgUniq   map[string]map[string]int64 // key is <queueName>, value is <msgId> => <expireat>

	maxBytes int64

	queuesPaused map[string]struct{} // key is <queueName>
}

// ListQueue implements Broker
func (it *BrokerInMemory) ListQueue(ctx context.Context) (rs []string, err error) {
	it.lock.RLock()
	defer it.lock.RUnlock()

	rs = make([]string, 0)
	for key := range it.listStat {
		rs = append(rs, key)
	}
	return rs, nil
}

// UTC implements Broker
func (it *BrokerInMemory) UTC(flag bool) {
	it.utc = flag
}

// ListFailed implements Broker
func (it *BrokerInMemory) ListFailed(ctx context.Context, queueName string, msgId string, limit int64, offset int64) (rs []*Msg, err error) {
	it.lock.RLock()
	defer it.lock.RUnlock()

	rs = make([]*Msg, 0)
	n := int64(0)
	o := int64(0)
	if msgHistory, ok := it.failedHistory[queueName]; ok {
		if l, ok := msgHistory[msgId]; ok {
			for e := l.Back(); e != nil; e = e.Prev() {
				if offset > 0 && o < offset {
					o += 1
					continue
				}

				msg := e.Value.(*Msg)
				rs = append(rs, msg)
				n += 1

				if n > limit {
					break
				}
			}
		}
	}

	return rs, nil
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
	it.listFailed = nil
	it.listStat = nil

	it.msgDetail = nil

	it.queuesPaused = nil

	return nil
}

// DeleteAgo implements Broker
func (it *BrokerInMemory) DeleteAgo(ctx context.Context, queueName string, duration time.Duration) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	now := it.getNow()
	nowInMs := now.UnixMilli()

	cutoff := now.Add(-duration).UnixMilli()

	for _, stateList := range []map[string]*list.List{
		it.listPending,
		it.listProcessing,
	} {
		if l, ok := stateList[queueName]; ok {
			for e := l.Front(); e != nil; e = e.Next() {
				msgId := e.Value.(string)

				msgs := it.msgDetail[queueName]
				rawMsg := msgs[msgId]

				if rawMsg.Expiredat > nowInMs {
					continue
				} else if rawMsg.Expiredat == 0 && rawMsg.Created > cutoff {
					continue
				}

				removeStringElementFromList(l, msgId)
				delete(msgs, msgId)
				delete(it.msgUniq[queueName], msgId)
			}
		}
	}

	msgHistory := it.failedHistory[queueName]
	for eMsgId := it.listFailed[queueName].Front(); eMsgId != nil; eMsgId = eMsgId.Next() {
		msgId := eMsgId.Value.(string)

		if l, ok := msgHistory[msgId]; ok {
			e := l.Front()
			if e != nil {
				rawMsg := e.Value.(*Msg)

				a := rawMsg.Expiredat > 0 && rawMsg.Expiredat < nowInMs
				b := rawMsg.Expiredat == 0 && rawMsg.Created < cutoff
				hasExpired := a || b
				if hasExpired {
					delete(msgHistory, msgId)

					it.listFailed[queueName].Remove(eMsgId)
				} else {
					for e := l.Front(); e != nil; e = e.Next() {
						rawMsg := e.Value.(*Msg)

						if rawMsg.Expiredat > nowInMs {
							continue
						} else if rawMsg.Expiredat == 0 && rawMsg.Created > cutoff {
							continue
						}

						l.Remove(e)
					}
				}
			}
		}
	}

	return nil
}

// DeleteMsg implements Broker
func (it *BrokerInMemory) DeleteMsg(ctx context.Context, queueName string, msgId string) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	if l, ok := it.listPending[queueName]; ok {
		removeStringElementFromList(l, msgId)
	}

	if l, ok := it.listProcessing[queueName]; ok {
		removeStringElementFromList(l, msgId)
	}

	if l, ok := it.listFailed[queueName]; ok {
		removeStringElementFromList(l, msgId)
	}

	delete(it.msgDetail[queueName], msgId)
	delete(it.msgUniq[queueName], msgId)
	delete(it.failedHistory[queueName], msgId)

	return nil
}

func removeStringElementFromList(l *list.List, b string) {
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

	delete(it.failedHistory, queueName)

	delete(it.listPending, queueName)
	delete(it.listProcessing, queueName)
	delete(it.listFailed, queueName)

	delete(it.msgDetail, queueName)
	delete(it.msgUniq, queueName)

	return nil
}

// Dequeue implements Broker
func (it *BrokerInMemory) Dequeue(ctx context.Context, queueName string) (*Msg, error) {
	it.lock.Lock()
	defer it.lock.Unlock()

	if _, ok := it.queuesPaused[queueName]; ok {
		return nil, ErrNoMsg
	}

	listPending, ok := it.listPending[queueName]
	if !ok {
		return nil, ErrNoMsg
	}

	e := listPending.Front()
	if e == nil {
		return nil, ErrNoMsg
	}

	msgId := e.Value.(string)
	listPending.Remove(e)

	it.listProcessing[queueName].PushBack(msgId)

	rawMsg := it.msgDetail[queueName][msgId]

	now := it.getNow()
	rawMsg.Updated = now.UnixMilli()
	rawMsg.State = MsgStateProcessing
	return rawMsg, nil
}

func (it *BrokerInMemory) getNow() time.Time {
	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	return now
}

// Enqueue implements Broker
func (it *BrokerInMemory) Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (*Msg, error) {
	it.lock.Lock()
	defer it.lock.Unlock()

	payload := msg.GetPayload()
	msgId := msg.GetId()
	if msgId == "" {
		msgId = uuid.NewString()
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

	now := it.getNow()
	nowInMs := now.UnixMilli()

	if uniqExpiredAt, ok := it.msgUniq[queueName][msgId]; ok {
		if uniqExpiredAt > 0 && uniqExpiredAt > nowInMs {
			return nil, ErrMsgIdConflict
		}
	}

	var expiredAt int64
	if uniqueInMs > 0 {
		expiredAt = now.Add(time.Millisecond * time.Duration(uniqueInMs)).UnixMilli()
		it.msgUniq[queueName][msgId] = expiredAt
	}

	if msg, ok := it.msgDetail[queueName][msgId]; ok {
		if msg.State == MsgStatePending || msg.State == MsgStateProcessing {
			return nil, ErrMsgIdConflict
		}
	}

	l := it.listPending[queueName]
	l.PushBack(msgId)

	rawMsg := &Msg{
		Created:   nowInMs,
		Expiredat: expiredAt,
		Id:        msgId,
		Payload:   payload,
		Queue:     queueName,
		State:     MsgStatePending,
		Updated:   nowInMs,
	}
	it.msgDetail[queueName][msgId] = rawMsg

	return rawMsg, nil
}

// Fail implements Broker
func (it *BrokerInMemory) Fail(ctx context.Context, msg IMsg, errFail error) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	queueName := msg.GetQueue()
	msgId := msg.GetId()

	msgs, ok := it.msgDetail[queueName]
	if !ok {
		return ErrNoMsg
	}
	rawMsg, ok := msgs[msgId]
	if !ok {
		return ErrNoMsg
	}

	rawMsg.Err = errFail.Error()
	rawMsg.State = MsgStateFailed

	now := it.getNow()
	nowInMs := now.UnixMilli()
	rawMsg.Updated = nowInMs

	if rawMsg.Expiredat == 0 {
		delete(it.msgDetail[queueName], msgId)
	} else if rawMsg.Expiredat > 0 && rawMsg.Expiredat < now.UnixMilli() {
		delete(it.msgDetail[queueName], msgId)
	}

	if l, ok := it.listProcessing[queueName]; ok {
		removeStringElementFromList(l, msgId)
	}
	it.listFailed[queueName].PushBack(msgId)

	msgHistory := it.failedHistory[queueName]
	if _, ok := msgHistory[msgId]; !ok {
		msgHistory[msgId] = list.New()
	}
	msgHistory[msgId].PushBack(rawMsg)

	if msgHistory[msgId].Len() > DefaultMaxItemsLimit {
		msgHistory[msgId].Remove(msgHistory[msgId].Front())
	}

	queueStat := it.listStat[queueName]
	todayYYYYMMDD := now.Format("2006-01-02")
	for e := queueStat.Back(); e != nil; e = e.Prev() {
		qds, ok := e.Value.(*QueueDailyStat)
		if !ok {
			continue
		}

		if todayYYYYMMDD == qds.Date {
			qds.Failed += 1
			return nil
		}
	}

	queueStat.PushBack(&QueueDailyStat{
		Date:      todayYYYYMMDD,
		Completed: 0,
		Failed:    1,
	})

	return nil
}

// Complete implements Broker
func (it *BrokerInMemory) Complete(ctx context.Context, msg IMsg) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	queueName := msg.GetQueue()
	msgId := msg.GetId()

	msgs, ok := it.msgDetail[queueName]
	if !ok {
		return ErrNoMsg
	}
	rawMsg, ok := msgs[msgId]
	if !ok {
		return ErrNoMsg
	}

	now := it.getNow()
	if rawMsg.Expiredat == 0 {
		delete(it.msgDetail[queueName], msgId)
	} else if rawMsg.Expiredat > 0 && rawMsg.Expiredat < now.UnixMilli() {
		delete(it.msgDetail[queueName], msgId)
	} else {
		rawMsg.State = MsgStateCompleted
		rawMsg.Updated = now.UnixMilli()
	}

	if l, ok := it.listProcessing[queueName]; ok {
		removeStringElementFromList(l, msgId)
	}

	queueStat := it.listStat[queueName]
	todayYYYYMMDD := now.Format("2006-01-02")
	for e := queueStat.Back(); e != nil; e = e.Prev() {
		qds, ok := e.Value.(*QueueDailyStat)
		if !ok {
			continue
		}

		if todayYYYYMMDD == qds.Date {
			qds.Completed += 1
			return nil
		}
	}

	// l is empty
	queueStat.PushBack(&QueueDailyStat{
		Date:      todayYYYYMMDD,
		Completed: 1,
		Failed:    0,
	})

	return nil
}

// GetMsg implements Broker
func (it *BrokerInMemory) GetMsg(ctx context.Context, queueName string, msgId string) (*Msg, error) {
	it.lock.RLock()
	defer it.lock.RUnlock()

	msgs, ok := it.msgDetail[queueName]
	if !ok {
		return nil, ErrNoMsg
	}

	if msg, ok := msgs[msgId]; !ok {
		return nil, ErrNoMsg
	} else {
		if msg.Expiredat > 0 && msg.Expiredat < it.getNow().UnixMilli() {
			return nil, ErrNoMsg
		}

		return msg, nil
	}
}

// GetStats implements Broker
func (it *BrokerInMemory) GetStats(ctx context.Context) ([]*QueueStat, error) {
	it.lock.RLock()
	defer it.lock.RUnlock()

	rs := make([]*QueueStat, 0)

	for _, queueName := range it.listQueues() {
		var pending, processing, failed, total int64

		if l, ok := it.listPending[queueName]; ok {
			pending = int64(l.Len())
		}

		if l, ok := it.listProcessing[queueName]; ok {
			processing = int64(l.Len())
		}

		if l, ok := it.listFailed[queueName]; ok {
			failed = int64(l.Len())
		}

		total = pending + processing + failed

		rs = append(rs, &QueueStat{
			Name:       queueName,
			Total:      total,
			Pending:    pending,
			Processing: processing,
			Failed:     failed,
		})
	}
	return rs, nil
}

func (it *BrokerInMemory) listQueues() (rs []string) {
	rs = make([]string, 0)

	for queueName := range it.listPending {
		rs = append(rs, queueName)
	}
	return rs
}

// GetStatsByDate implements Broker
func (it *BrokerInMemory) GetStatsByDate(ctx context.Context, YYYYMMDD string) (*QueueDailyStat, error) {
	it.lock.RLock()
	defer it.lock.RUnlock()

	now := it.getNow()
	todayYYYYMMDD := now.Format("2006-01-02")

	rs := &QueueDailyStat{Date: todayYYYYMMDD}
	for _, queueName := range it.listQueues() {
		if l, ok := it.listStat[queueName]; ok {
			for e := l.Back(); e != nil; e = e.Prev() {
				qds, ok := e.Value.(*QueueDailyStat)
				if !ok {
					continue
				}

				if qds.Date != todayYYYYMMDD {
					continue
				}

				rs.Completed += qds.Completed
				rs.Failed += qds.Failed
				rs.Total += qds.Completed + qds.Failed
			}
		}
	}

	return rs, nil
}

// GetStatsWeekly implements Broker
func (it *BrokerInMemory) GetStatsWeekly(ctx context.Context) ([]*QueueDailyStat, error) {
	rs := make([]*QueueDailyStat, 0)
	date := it.clock.Now().AddDate(0, 0, -7)
	for i := 0; i <= 7; i++ {
		rsOneDay, err := it.GetStatsByDate(ctx, gtime.UnixTime2YyyymmddUtc(date.Unix()))
		if err != nil {
			return nil, ErrInternal
		}
		rs = append(rs, rsOneDay)
		date = date.AddDate(0, 0, 1)
	}
	return rs, nil
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
			break
		}
	case MsgStateProcessing:
		{
			l, ok = it.listProcessing[queueName]
			break
		}
	case MsgStateFailed:
		{
			l, ok = it.listFailed[queueName]
			break
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

		if limit > 0 && n > int(limit) {
			break
		}
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
	if _, ok := it.listPending[queueName]; !ok {
		it.listPending[queueName] = list.New()
		it.listProcessing[queueName] = list.New()
		it.listFailed[queueName] = list.New()

		it.listStat[queueName] = list.New()

		it.failedHistory[queueName] = make(MsgHistory)

		it.msgDetail[queueName] = make(map[string]*Msg, 0)
		it.msgUniq[queueName] = make(map[string]int64)
	}

	return nil
}

func NewBrokerInMemory(opts *BrokerInMemoryOpts) (rs Broker, err error) {
	broker := &BrokerInMemory{
		clock:          NewWallClock(),
		lock:           sync.RWMutex{},
		failedHistory:  make(map[string]MsgHistory),
		listPending:    make(map[string]*list.List),
		listProcessing: make(map[string]*list.List),
		listFailed:     make(map[string]*list.List),
		listStat:       make(map[string]*list.List),
		queuesPaused:   make(map[string]struct{}),
		msgDetail:      make(map[string]map[string]*Msg),
		msgUniq:        make(map[string]map[string]int64),
		maxBytes:       DefaultMaxBytes,
	}
	if opts != nil {
		broker.maxBytes = opts.MaxBytes
	}
	return broker, nil
}
