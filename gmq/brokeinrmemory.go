package gmq

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/giant-stone/go/gtime"
	"github.com/google/uuid"
)

const (
	DefaultMaxBytes = 1024 * 1024 * 32 // 32 MB
)

type BrokerInMemory struct {
	BrokerUnimplemented

	clock Clock
	utc   bool
	lock  sync.RWMutex

	listPending    map[string]*list.List // key is queueName
	listWaiting    map[string]*list.List
	listProcessing map[string]*list.List
	listCompleted  map[string]*list.List
	listFailed     map[string]*list.List

	listStat map[string]*list.List // key is queueName, list.Element value is *QueueDailyStat

	msgDetail map[string]*Msg // key is msgId

	maxBytes int64

	queuesPaused map[string]struct{} // key is queueName
}

// UTC implements Broker
func (it *BrokerInMemory) UTC(flag bool) {
	it.utc = flag
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
	it.listStat = nil

	it.msgDetail = nil

	it.queuesPaused = nil

	return nil
}

func newMsgId(queueName, id string) string {
	return fmt.Sprintf("%s:%s", queueName, id)
}

// Complete implements Broker
func (it *BrokerInMemory) Complete(ctx context.Context, msg IMsg) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	queueName := msg.GetQueue()
	msgId := newMsgId(queueName, msg.GetId())
	rawMsg, ok := it.msgDetail[msgId]
	if !ok {
		return ErrNoMsg
	}

	rawMsg.State = MsgStateCompleted

	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	nowInMs := now.UnixMilli()
	rawMsg.Updated = nowInMs

	if l, ok := it.listProcessing[queueName]; ok {
		removeElementFromList(l, msgId)
	}

	it.listCompleted[queueName].PushBack(msgId)

	today := it.clock.Now()
	todayYYYYMMDD := today.Format("2006-01-02")
	l, ok := it.listStat[queueName]
	if !ok {
		it.listStat[queueName] = list.New()
	}

	for e := l.Back(); e != nil; e = e.Prev() {
		qds, ok := e.Value.(*QueueDailyStat)
		if !ok {
			continue
		}
		tsStat, _ := gtime.Yyyymmdd2unixTimeUtc(qds.Date)
		tsToday, _ := gtime.Yyyymmdd2unixTimeUtc(todayYYYYMMDD)
		if tsStat == tsToday {
			qds.Completed += 1
			return nil
		} else if tsStat < tsToday {
			l.PushBack(&QueueDailyStat{
				Date:      todayYYYYMMDD,
				Completed: 1,
				Failed:    0,
			})
			return nil
		}
	}

	// l is empty
	l.PushBack(&QueueDailyStat{
		Date:      todayYYYYMMDD,
		Completed: 1,
		Failed:    0,
	})

	return nil
}

// DeleteAgo implements Broker
func (it *BrokerInMemory) DeleteAgo(ctx context.Context, queueName string, duration time.Duration) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	nowInMs := now.UnixMilli()

	for _, stateList := range []map[string]*list.List{
		it.listPending,
		it.listWaiting,
		it.listProcessing,
		it.listCompleted,
		it.listFailed,
	} {
		if l, ok := stateList[queueName]; ok {
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

	msgId := newMsgId(queueName, id)

	if l, ok := it.listPending[queueName]; ok {
		removeElementFromList(l, msgId)
	}

	if l, ok := it.listWaiting[queueName]; ok {
		removeElementFromList(l, msgId)
	}

	if l, ok := it.listProcessing[queueName]; ok {
		removeElementFromList(l, msgId)
	}

	if l, ok := it.listCompleted[queueName]; ok {
		removeElementFromList(l, msgId)
	}

	if l, ok := it.listFailed[queueName]; ok {
		removeElementFromList(l, msgId)
	}

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

	for _, stateList := range []map[string]*list.List{
		it.listPending,
		it.listWaiting,
		it.listProcessing,
		it.listCompleted,
		it.listFailed,
	} {
		if l, ok := stateList[queueName]; ok {
			for e := l.Front(); e != nil; e = e.Next() {
				msgId := e.Value.(string)

				removeElementFromList(l, msgId)
				delete(it.msgDetail, msgId)

				delete(stateList, queueName)
			}
		}
	}

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

	if listProcessing, ok := it.listProcessing[queueName]; !ok {
		return nil, ErrNoMsg
	} else {
		listProcessing.PushBack(msgId)
	}

	rawMsg := it.msgDetail[msgId]

	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

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
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	nowInMs := now.UnixMilli()
	msgId := newMsgId(queueName, id)

	if msg, ok := it.msgDetail[msgId]; ok {
		if msg.Expiredat > nowInMs {
			return nil, ErrMsgIdConflict
		}
	}

	l := it.listPending[queueName]
	l.PushBack(msgId)

	var expiredAt int64
	if uniqueInMs > 0 {
		expiredAt = now.Add(time.Millisecond * time.Duration(uniqueInMs)).UnixMilli()
	}

	rawMsg := &Msg{
		Created:   nowInMs,
		Expiredat: expiredAt,
		Id:        id,
		Payload:   payload,
		Queue:     queueName,
		State:     MsgStatePending,
		Updated:   nowInMs,
	}
	it.msgDetail[msgId] = rawMsg

	return rawMsg, nil
}

// Fail implements Broker
func (it *BrokerInMemory) Fail(ctx context.Context, msg IMsg, errFail error) error {
	it.lock.Lock()
	defer it.lock.Unlock()

	queueName := msg.GetQueue()
	msgId := newMsgId(queueName, msg.GetId())
	rawMsg, ok := it.msgDetail[msgId]
	if !ok {
		return ErrNoMsg
	}

	rawMsg.Err = errFail.Error()
	rawMsg.State = MsgStateFailed
	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	nowInMs := now.UnixMilli()
	rawMsg.Updated = nowInMs

	it.listFailed[queueName].PushBack(msgId)

	l, ok := it.listStat[queueName]
	if !ok {
		it.listStat[queueName] = list.New()
	}

	today := it.clock.Now()
	todayYYYYMMDD := today.Format("2006-01-02")
	for e := l.Back(); e != nil; e = e.Prev() {
		qds, ok := e.Value.(*QueueDailyStat)
		if !ok {
			continue
		}
		tsStat, _ := gtime.Yyyymmdd2unixTimeUtc(qds.Date)
		tsToday, _ := gtime.Yyyymmdd2unixTimeUtc(todayYYYYMMDD)
		if tsStat == tsToday {
			qds.Failed += 1
			return nil
		} else if tsStat < tsToday {
			l.PushBack(&QueueDailyStat{
				Date:      todayYYYYMMDD,
				Completed: 0,
				Failed:    1,
			})
			return nil
		}
	}

	// l is empty
	l.PushBack(&QueueDailyStat{
		Date:      todayYYYYMMDD,
		Completed: 0,
		Failed:    1,
	})

	return nil
}

// GetMsg implements Broker
func (it *BrokerInMemory) GetMsg(ctx context.Context, queueName string, id string) (*Msg, error) {
	it.lock.RLock()
	defer it.lock.RUnlock()

	msgId := newMsgId(queueName, id)
	if msg, ok := it.msgDetail[msgId]; !ok {
		return nil, ErrNoMsg
	} else {
		return msg, nil
	}
}

// GetStats implements Broker
func (it *BrokerInMemory) GetStats(ctx context.Context) ([]*QueueStat, error) {
	it.lock.RLock()
	defer it.lock.RUnlock()

	rs := make([]*QueueStat, 0)

	for _, queueName := range it.listQueues() {
		var pending, waiting, processing, completed, failed, total int64

		if l, ok := it.listPending[queueName]; ok {
			pending = int64(l.Len())
		}

		if l, ok := it.listWaiting[queueName]; ok {
			waiting = int64(l.Len())
		}

		if l, ok := it.listProcessing[queueName]; ok {
			processing = int64(l.Len())
		}

		if l, ok := it.listCompleted[queueName]; ok {
			completed = int64(l.Len())
		}

		if l, ok := it.listFailed[queueName]; ok {
			failed = int64(l.Len())
		}

		total = pending + waiting + processing + completed + failed

		rs = append(rs, &QueueStat{
			Name:       queueName,
			Total:      total,
			Pending:    pending,
			Waiting:    waiting,
			Processing: processing,
			Completed:  completed,
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

	today := it.clock.Now()
	todayYYYYMMDD := today.Format("2006-01-02")

	rs := &QueueDailyStat{Date: todayYYYYMMDD}
	for _, queueName := range it.listQueues() {
		if l, ok := it.listStat[queueName]; ok {
			for e := l.Back(); e != nil; e = e.Prev() {
				qds, ok := e.Value.(*QueueDailyStat)
				if !ok {
					continue
				}

				tsStat, _ := gtime.Yyyymmdd2unixTimeUtc(qds.Date)
				tsToday, _ := gtime.Yyyymmdd2unixTimeUtc(todayYYYYMMDD)
				if tsStat != tsToday {
					continue
				}

				rs.Completed += qds.Completed
				rs.Failed += qds.Failed
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
	if _, ok := it.listPending[queueName]; !ok {
		it.listPending[queueName] = list.New()
	}

	if _, ok := it.listWaiting[queueName]; !ok {
		it.listWaiting[queueName] = list.New()
	}

	if _, ok := it.listProcessing[queueName]; !ok {
		it.listProcessing[queueName] = list.New()
	}

	if _, ok := it.listCompleted[queueName]; !ok {
		it.listCompleted[queueName] = list.New()
	}

	if _, ok := it.listFailed[queueName]; !ok {
		it.listFailed[queueName] = list.New()
	}

	if _, ok := it.listStat[queueName]; !ok {
		it.listStat[queueName] = list.New()
	}
	return nil
}

func NewBrokerInMemory(opts *BrokerInMemoryOpts) (rs Broker, err error) {
	broker := &BrokerInMemory{
		clock:          NewWallClock(),
		lock:           sync.RWMutex{},
		listPending:    make(map[string]*list.List),
		listWaiting:    make(map[string]*list.List),
		listProcessing: make(map[string]*list.List),
		listCompleted:  make(map[string]*list.List),
		listFailed:     make(map[string]*list.List),
		listStat:       make(map[string]*list.List),
		queuesPaused:   make(map[string]struct{}),
		msgDetail:      make(map[string]*Msg),
		maxBytes:       DefaultMaxBytes,
	}
	if opts != nil {
		broker.maxBytes = opts.MaxBytes
	}
	return broker, nil
}
