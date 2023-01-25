package gmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/giant-stone/go/gstr"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

const (
	LuaReturnCodeSucc = iota // confirm to POSIX shell/C return code common rule, 0 means successfully
	LuaReturnCodeError
)

func NewKeyQueueList() string {
	return fmt.Sprintf("%s:%s", Namespace, QueueNameList)
}

func NewKeyQueuePending(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, MsgStatePending)
}

func NewKeyMsgDetail(ns, queueName, msgId string) string {
	return fmt.Sprintf("%s:%s:msg:%s", ns, queueName, msgId)
}

func NewKeyMsgUnique(ns, queueName, msgId string) string {
	return fmt.Sprintf("%s:%s:uniq:%s", ns, queueName, msgId)
}

func NewKeyQueuePaused(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, QueueNamePaused)
}

func NewKeyQueueProcessing(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, MsgStateProcessing)
}

func NewKeyQueueCompleted(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, MsgStateCompleted)
}

func NewKeyQueueFailed(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, MsgStateFailed)
}

// <namespace>:<queueName>:his:<msgId>
func NewKeyQueueFailedHistory(ns, queueName, msgId string) string {
	return fmt.Sprintf("%s:%s:his:%s", ns, queueName, msgId)
}

// <namespace>:<queueName>:completed:<YYYY-MM-DD>
func NewKeyDailyStatCompleted(ns, queueName, YYYYMMDD string) string {
	return fmt.Sprintf("%s:%s", NewKeyQueueCompleted(ns, queueName), YYYYMMDD)
}

// <namespace>:<queueName>:failed:<YYYY-MM-DD>
func NewKeyDailyStatFailed(ns, queueName, YYYYMMDD string) string {
	return fmt.Sprintf("%s:%s", NewKeyQueueFailed(ns, queueName), YYYYMMDD)
}

func NewKeyQueueState(ns, queueName string, state string) string {
	return fmt.Sprintf("%s:%s:%s", ns, queueName, state)
}

func NewKeyQueuePattern(ns, queueName string) string {
	return fmt.Sprintf("%s:%s:*", ns, queueName)
}

func NewBrokerRedis(dsn string) (rs Broker, err error) {
	opts, err := redis.ParseURL(dsn)
	if err != nil {
		return
	}

	cli, _ := MakeRedisUniversalClient(opts).(redis.UniversalClient)
	return &BrokerRedis{cli: cli, clock: NewWallClock(), namespace: Namespace}, nil
}

func NewBrokerFromRedisClient(cli redis.UniversalClient) (rs Broker, err error) {
	return &BrokerRedis{cli: cli, clock: NewWallClock(), namespace: Namespace}, nil
}

func MakeRedisUniversalClient(opts *redis.Options) (rs interface{}) {
	return redis.NewClient(opts)
}

type BrokerRedis struct {
	BrokerUnimplemented

	cli       redis.UniversalClient
	clock     Clock
	namespace string

	utc bool
}

func (it *BrokerRedis) getNow() time.Time {
	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	return now
}

// ListQueue implements Broker
func (it *BrokerRedis) ListQueue(ctx context.Context) (rs []string, err error) {
	return it.listQueues(ctx)
}

func (it *BrokerRedis) ListFailed(ctx context.Context, queueName string, msgId string, limit int64, offset int64) (rs []*Msg, err error) {
	if limit <= 0 {
		limit = DefaultMaxItemsLimit - 1
	}
	if offset <= 0 {
		offset = 0
	}

	key := NewKeyQueueFailedHistory(it.namespace, queueName, msgId)
	rs = make([]*Msg, 0)

	items, err := it.cli.LRange(ctx, key, offset, limit-1).Result()
	if err != nil {
		if err == redis.ErrClosed {
			return nil, nil
		}
		return nil, err
	}

	for _, item := range items {
		msg := &Msg{}
		err = json.Unmarshal([]byte(item), msg)
		if err != nil {
			return nil, err
		}
		msg.Queue = queueName
		msg.Id = msgId
		rs = append(rs, msg)
	}

	return rs, nil
}

// UTC implements Broker
func (it *BrokerRedis) UTC(flag bool) {
	it.utc = flag
}

func (it *BrokerRedis) Ping(ctx context.Context) error {
	return it.cli.Ping(ctx).Err()
}

func (it *BrokerRedis) Close() error {
	return it.cli.Close()
}

func (it *BrokerRedis) Init(ctx context.Context, queueName string) (err error) {
	return it.updateQueueList(ctx, queueName)
}

func (it *BrokerRedis) updateQueueList(ctx context.Context, queueName string) (err error) {
	_, err = it.cli.SAdd(ctx, NewKeyQueueList(), queueName).Result()
	return
}

// script(Enqueue) enqueues a message.
//
// Input:
// KEYS[1] -> <namespace>:<queueName>:msg:<msgId>
// KEYS[2] -> <namespace>:<queueName>:pending
// --
// ARGV[1] -> <message payload>
// ARGV[2] -> "pending"
// ARGV[3] -> <current unix time in milliseconds>
// ARGV[4] -> <msgId>
//
// Output:
// Returns 0 if successfully enqueued
// Returns 1 if message ID already exists
//
//	list item order from left to right, head to tail, fresh to old.
var scriptEnqueue = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 1
end
redis.call("HSET", KEYS[1],
           "payload", ARGV[1],
           "state",   ARGV[2],
           "created", ARGV[3],
					 "updated", ARGV[3],
					 "expireat", 0)
redis.call("LPUSH", KEYS[2], ARGV[4])
return 0
`)

// scriptEnqueueUnique enqueues a message with unique in duration constraint
//
// Input:
// KEYS[1] -> <namespace>:<queueName>:uniq:<msgId>
// KEYS[2] -> <namespace>:<queueName>:msg:<msgId>
// KEYS[3] -> <namespace>:<queueName>:pending
// --
// ARGV[1] -> <msg unique in duration in milliseconds>
// ARGV[2] -> <message payload>
// ARGV[3] -> "pending"
// ARGV[4] -> <current unix time in milliseconds>
// ARGV[5] -> <expire at unix time in milliseconds>
// ARGV[6] -> <msgId>
//
// Output:
// Returns 0 if successfully enqueued
// Returns 1 if message ID already exists
var scriptEnqueueUnique = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 1
end
redis.call("PSETEX", KEYS[1], ARGV[1], ARGV[5])
redis.call("HSET", KEYS[2],
           "payload", ARGV[2],
           "state",   ARGV[3],
           "created", ARGV[4],
					 "updated", ARGV[4],
					 "expireat", ARGV[5])
redis.call("RPUSH", KEYS[3], ARGV[6])
return 0
`)

func (it *BrokerRedis) Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (rs *Msg, err error) {
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

	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	nowInMs := now.UnixMilli()
	var expiredAt int64
	var resI interface{}
	if uniqueInMs == 0 {
		keys := []string{
			NewKeyMsgDetail(it.namespace, queueName, msgId),
			NewKeyQueuePending(it.namespace, queueName),
		}

		args := []interface{}{
			payload,
			MsgStatePending,
			nowInMs,
			msgId,
		}
		resI, err = scriptEnqueue.Run(ctx, it.cli, keys, args...).Result()
	} else {
		expiredAt = now.Add(time.Millisecond * time.Duration(uniqueInMs)).UnixMilli()

		keys := []string{
			NewKeyMsgUnique(it.namespace, queueName, msgId),
			NewKeyMsgDetail(it.namespace, queueName, msgId),
			NewKeyQueuePending(it.namespace, queueName),
		}

		args := []interface{}{
			uniqueInMs,
			payload,
			MsgStatePending,
			nowInMs,
			expiredAt,
			msgId,
		}
		resI, err = scriptEnqueueUnique.Run(ctx, it.cli, keys, args...).Result()
	}

	if err != nil {
		return
	}

	rt, ok := resI.(int64)
	if !ok {
		err = ErrInternal
		return
	}

	if rt == LuaReturnCodeError {
		err = ErrMsgIdConflict
		return
	}

	return &Msg{
		Created:   nowInMs,
		Expiredat: expiredAt,
		Id:        msgId,
		Payload:   payload,
		Queue:     queueName,
		State:     MsgStatePending,
		Updated:   nowInMs,
	}, nil
}

// scriptDequeue dequeues a message.
//
// Input:
// KEYS[1] -> <namespace>:<queueName>:pending
// KEYS[2] -> <namespace>:<queueName>:paused
// KEYS[3] -> <namespace>:<queueName>:processing
// --
// ARGV[1] -> message detail key prefix
// ARGV[2] -> state "processing"
// ARGV[3] -> update at <current unix time in milliseconds>
//
// Output:
// Returns nil if no processable message is found in the given queue.
// Returns an encoded Message.
var scriptDequeue = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 0 then
	local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
	if id then
		local key = ARGV[1] .. id
		redis.call("HSET", key, "state", ARGV[2], "updated", ARGV[3])
		return {id, redis.call("HGETALL", key)}
	end
end
return nil`)

func (it *BrokerRedis) Dequeue(ctx context.Context, queueName string) (msg *Msg, err error) {
	nowInUnixMilli := it.getNow().UnixMilli()

	keys := []string{
		NewKeyQueuePending(it.namespace, queueName),
		NewKeyQueuePaused(it.namespace, queueName),
		NewKeyQueueProcessing(it.namespace, queueName),
	}
	args := []interface{}{
		NewKeyMsgDetail(it.namespace, queueName, ""),
		MsgStateProcessing,
		nowInUnixMilli,
	}

	resI, err := scriptDequeue.Run(ctx, it.cli, keys, args...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNoMsg
		}
		return nil, err
	}

	res, ok := resI.([]interface{})
	if !ok {
		// TODO: move this message into internal damaged queue
		return nil, ErrInternal
	}

	if len(res) != 2 {
		// TODO: move this message into internal damaged queue
		return nil, ErrInternal
	}

	msgId, _ := res[0].(string)
	if msgId == "" {
		return nil, ErrInternal
	}

	rawMsg, err := parseMsgFromRedisLuaHgetallResult(res[1])
	if err != nil {
		return nil, err
	}

	rawMsg.Id = msgId
	rawMsg.Queue = queueName
	rawMsg.State = MsgStateProcessing
	rawMsg.Updated = nowInUnixMilli
	return rawMsg, nil
}

// scriptDeleteQueue delete a queue
// KEYS[1] -> <namespace>:<queuename>:*
var scriptDeleteQueue = redis.NewScript(`
	for k,v in ipairs(KEYS) do 
		redis.call('del', KEYS[k])
	end
	return nil
`)

func (it *BrokerRedis) DeleteQueue(ctx context.Context, queueName string) (err error) {
	key := NewKeyQueuePattern(it.namespace, queueName)
	keys, err := it.cli.Keys(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		err = ErrInternal
		return
	}
	_, err = scriptDeleteQueue.Run(ctx, it.cli, keys, 0).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		err = ErrInternal
		return
	}
	return
}

// scriptDeleteMsg delete a message.
//
// KEYS[1] -> <namespace>:<queueName>:pending
// KEYS[2] -> <namespace>:<queueName>:processing
// KEYS[3] -> <namespace>:<queueName>:failed
// KEYS[4] -> <namespace>:<queueName>:msg:<msgId>
// KEYS[5] -> <namespace>:<queueName>:uniq:<msgId>
// KEYS[6] -> <namespace>:<queueName>:his:<msgId>
//
// ARGV[1] -> <msgId>
var scriptDeleteMsg = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("LREM", KEYS[2], 0, ARGV[1])
redis.call("LREM", KEYS[3], 0, ARGV[1])
redis.call("DEL", KEYS[4])
redis.call("DEL", KEYS[5])
redis.call("DEL", KEYS[6])
return 0
`)

func (it *BrokerRedis) DeleteMsg(ctx context.Context, queueName, msgId string) (err error) {
	keys := []string{
		NewKeyQueuePending(it.namespace, queueName),
		NewKeyQueueProcessing(it.namespace, queueName),
		NewKeyQueueFailed(it.namespace, queueName),
		NewKeyMsgDetail(it.namespace, queueName, msgId),
		NewKeyMsgUnique(it.namespace, queueName, msgId),
		NewKeyQueueFailedHistory(it.namespace, queueName, msgId),
	}

	argv := []interface{}{
		msgId,
	}

	resI, err := scriptDeleteMsg.Run(ctx, it.cli, keys, argv...).Result()
	if err != nil {
		return err
	}

	rt, ok := resI.(int64)
	if !ok {
		return ErrInternal
	}

	if rt == LuaReturnCodeError {
		return ErrInternal
	}
	return nil
}

// scriptDeleteAgo delete messages old than <nowUnixTimestamp - duration>.
//
// KEYS[1] -> <namespace>:<queueName>:pending
// KEYS[2] -> <namespace>:<queueName>:processing
// KEYS[i]   -> <msgId1>
// KEYS[i+1] -> <keyMsgDetail1>
// ...
//
// ARGV[1] -> cutoff
// ARGV[2] -> Length of KEYS
// ARGV[3] -> "created"
// ARGV[4] -> "expireat"
var scriptDeleteAgo = redis.NewScript(`
local totalKeys = tonumber(ARGV[2])
for i=6, totalKeys, 2 do
	local keyMsgDetail = KEYS[i+1]
	local created = redis.call("HGET", keyMsgDetail, ARGV[3])
	local expireat = redis.call("HGET", keyMsgDetail, ARGV[4])
	if (created and tonumber(created) <= tonumber(ARGV[1])) or (expireat and tonumber(expireat) <= tonumber(ARGV[1])) then
		local msgId = KEYS[i]
		redis.call("DEL", keyMsgDetail)
		redis.call("LREM", KEYS[1], 0, msgId)
		redis.call("LREM", KEYS[2], 0, msgId)
	end
end
return 0
`)

// delete messages old than <nowUnixTimestamp - duration>
func (it *BrokerRedis) DeleteAgo(ctx context.Context, queueName string, duration time.Duration) error {
	now := it.getNow()
	cutoff := now.Add(-duration).UnixMilli()

	states := []string{
		NewKeyQueuePending(it.namespace, queueName),
		NewKeyQueueProcessing(it.namespace, queueName),
	}

	limit := int64(1000)

	for _, state := range states {
		for {
			keys := []string{
				states[0],
				states[1],
				"created",
				"expireat",
			}

			msgIds, err := it.cli.LRange(ctx, state, 0, limit).Result()
			if err != nil {
				if err == redis.ErrClosed {
					return nil
				}
				return err
			}

			if len(msgIds) == 0 {
				break
			}

			for i := range msgIds {
				msgId := msgIds[i]
				keys = append(keys, msgId, NewKeyMsgDetail(it.namespace, queueName, msgId))
			}

			if len(keys) == 4 {
				break
			}

			args := []interface{}{
				cutoff,
				len(keys),
			}

			_, err = scriptDeleteAgo.Run(ctx, it.cli, keys, args).Result()
			if err != nil {
				return err
			}
		}
	}

	keyFailed := NewKeyQueueFailed(it.namespace, queueName)
	for {
		msgIds, err := it.cli.LRange(ctx, keyFailed, 0, limit).Result()
		if err != nil {
			return err
		}

		if len(msgIds) == 0 {
			break
		}

		for _, msgId := range msgIds {
			keyFailedHis := NewKeyQueueFailedHistory(it.namespace, queueName, msgId)

			items, err := it.cli.LRange(ctx, keyFailedHis, 0, limit).Result()
			if err != nil {
				if err == redis.ErrClosed {
					return nil
				}
				return err
			}
			if len(items) == 0 {
				break
			}

			rawMsg := &Msg{}
			err = json.Unmarshal([]byte(items[0]), rawMsg)
			if err != nil {
				return err
			}

			// delete this mesasge all history
			if (rawMsg.Created > 0 && rawMsg.Created < cutoff) || (rawMsg.Expiredat > 0 && rawMsg.Expiredat <= cutoff) {
				_, err := it.cli.Del(ctx, keyFailedHis).Result()
				if err != nil {
					return err
				}
				_, err = it.cli.LRem(ctx, keyFailed, 0, msgId).Result()
				if err != nil {
					return err
				}
				continue
			}

			// delete this mesasge expired history records
			for _, item := range items {
				msg := &Msg{}
				err = json.Unmarshal([]byte(item), msg)
				if err != nil {
					return err
				}

				if (rawMsg.Created > 0 && rawMsg.Created < cutoff) || (rawMsg.Expiredat > 0 && rawMsg.Expiredat <= cutoff) {
					it.cli.LRem(ctx, keyFailedHis, 0, item)
				}
			}
		}
	}

	return nil
}

func (it *BrokerRedis) Pause(ctx context.Context, qname string) error {
	key := NewKeyQueuePaused(it.namespace, qname)

	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}
	ok, err := it.cli.SetNX(ctx, key, now.Unix(), 0).Result()
	if err != nil {
		return err
	}
	if !ok {
		return ErrInternal
	}

	return nil
}

func (it *BrokerRedis) Resume(ctx context.Context, qname string) error {
	key := NewKeyQueuePaused(it.namespace, qname)
	_, err := it.cli.Del(ctx, key).Result()
	if err != nil {
		return err
	}
	return nil
}

// scriptComplete marks a message consumed successfully.
//
// KEYS[1] -> <namespace>:<queueName>:processing
// KEYS[2] -> <namespace>:<queueName>:msg:<msgId>
// KEYS[3] -> <namespace>:<queueName>:completed:<YYYY-MM-DD>
//
// ARGV[1] -> <msgId>
var scriptComplete = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
	return 1
end

if redis.call("DEL", KEYS[2]) == 0 then
	return 1
end

redis.call("INCR", KEYS[3])
return 0
`)

func (it *BrokerRedis) Complete(ctx context.Context, msg IMsg) (err error) {
	queueName := msg.GetQueue()
	msgId := msg.GetId()

	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	keys := []string{
		NewKeyQueueProcessing(it.namespace, queueName),
		NewKeyMsgDetail(it.namespace, queueName, msgId),
		NewKeyDailyStatCompleted(it.namespace, queueName, now.Format("2006-01-02")),
	}
	args := []interface{}{
		msgId,
	}

	resI, err := scriptComplete.Run(ctx, it.cli, keys, args...).Result()
	if err != nil {
		if err == redis.Nil {
			err = ErrNoMsg
			return
		}
		err = ErrInternal
		return
	}

	rt, ok := resI.(int64)
	if !ok {
		err = ErrInternal
		return
	}

	if rt == LuaReturnCodeError {
		err = ErrNoMsg
		return
	}

	return
}

func (it *BrokerRedis) SetClock(c Clock) {
	it.clock = c
}

func (it *BrokerRedis) GetMsg(ctx context.Context, queueName, msgId string) (msg *Msg, err error) {
	values, err := it.cli.HGetAll(ctx, NewKeyMsgDetail(it.namespace, queueName, msgId)).Result()
	if err != nil {
		return
	}

	if len(values) == 0 {
		err = ErrNoMsg
		return
	}

	payload, ok := values["payload"]
	if !ok {
		err = ErrInternal
		return
	}

	return &Msg{
		Payload: []byte(payload),
		Id:      msgId,
		Queue:   queueName,
		Created: gstr.Atoi64(values["created"]),
		Err:     values["err"],
		State:   values["state"],
		Updated: gstr.Atoi64(values["updated"]),
	}, nil
}

func (it *BrokerRedis) ListMsg(ctx context.Context, queueName, state string, offset, limit int64) (values []string, err error) {
	if limit <= 0 {
		limit = DefaultMaxItemsLimit - 1
	}
	if offset <= 0 {
		offset = 0
	}

	values, err = it.cli.LRange(ctx, NewKeyQueueState(it.namespace, queueName, state), offset, limit-1).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
	}
	return values, nil
}

type QueueStat struct {
	Name       string
	Total      int64 // all state of message store in Redis
	Pending    int64 // wait to free worker consume it
	Processing int64 // worker already took and consuming
	Failed     int64 // occured error, and/or pending to retry
}

func (it *BrokerRedis) listQueues(ctx context.Context) (rs []string, err error) {
	reply, err := it.cli.Do(ctx, "smembers", NewKeyQueueList()).Slice()
	if err != nil {
		return
	}
	rs = make([]string, 0)
	for _, itemI := range reply {
		item, ok := itemI.(string)
		if !ok {
			continue
		}
		rs = append(rs, item)
	}
	return
}

func (it *BrokerRedis) GetStats(ctx context.Context) (rs []*QueueStat, err error) {
	queueNames, err := it.listQueues(ctx)
	if err != nil {
		return
	}

	rs = make([]*QueueStat, 0)
	for _, queueName := range queueNames {
		pending, _ := it.cli.LLen(ctx, NewKeyQueuePending(it.namespace, queueName)).Result()
		processing, _ := it.cli.LLen(ctx, NewKeyQueueProcessing(it.namespace, queueName)).Result()
		failed, _ := it.cli.LLen(ctx, NewKeyQueueFailed(it.namespace, queueName)).Result()

		total := pending + processing + failed

		rs = append(rs, &QueueStat{
			Name:       queueName,
			Total:      total,
			Pending:    pending,
			Processing: processing,
			Failed:     failed,
		})
	}
	return
}

type QueueDailyStat struct {
	Date      string // YYYY-MM-DD in UTC
	Completed int64
	Failed    int64
	Total     int64 // it is equal to Completed + Failed
}

func (it *BrokerRedis) GetStatsWeekly(ctx context.Context) ([]*QueueDailyStat, error) {
	now := it.clock.Now()
	if it.utc {
		now = now.UTC()
	} else {
		now = now.Local()
	}

	rs := make([]*QueueDailyStat, 0)
	date := now.AddDate(0, 0, -7)
	for i := 0; i <= 7; i++ {
		rsOneDay, err := it.GetStatsByDate(ctx, date.Format("2006-01-02"))
		if err != nil {
			return nil, ErrInternal
		}
		rs = append(rs, rsOneDay)
		date = date.AddDate(0, 0, 1)
	}
	return rs, nil
}

func (it *BrokerRedis) GetStatsByDate(ctx context.Context, date string) (rs *QueueDailyStat, err error) {
	queueNames, err := it.listQueues(ctx)
	if err != nil {
		return
	}

	rs = &QueueDailyStat{Date: date}
	for _, queueName := range queueNames {
		completed, _ := it.cli.Get(ctx, NewKeyDailyStatCompleted(it.namespace, queueName, date)).Result()
		failed, _ := it.cli.Get(ctx, NewKeyDailyStatFailed(it.namespace, queueName, date)).Result()

		if completed != "" {
			value := gstr.Atoi64(completed)
			if value > 0 {
				rs.Completed += value
			}
		}

		if failed != "" {
			value := gstr.Atoi64(failed)
			if value > 0 {
				rs.Failed += value
			}
		}

		rs.Total = rs.Completed + rs.Failed
	}

	return
}

// scriptFail enqueues a failed message
// Input:
// KEYS[1] -> <namespace>:<queueName>:msg:<msgId>
// KEYS[2] -> <namespace>:<queueName>:processing
// KEYS[3] -> <namespace>:<queueName>:failed
// KEYS[4] -> <namespace>:<queueName>:failed:<YYYY-MM-DD>
// --
// ARGV[1] -> <msgId>
//
// Output:
// Returns message detail if successfully
// Returns nil if message matched msgId not found
var scriptFail = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	redis.call("LREM", KEYS[2], 0, ARGV[1])
	redis.call("LPUSH", KEYS[3], ARGV[1])
	redis.call("INCR", KEYS[4])

	local msg = redis.call('HGETALL', KEYS[1])
	redis.call('DEL', KEYS[1])
	return msg
end
return nil
`)

func (it *BrokerRedis) Fail(ctx context.Context, msg IMsg, errFail error) (err error) {
	msgId := msg.GetId()

	now := it.getNow()

	today := now.Format("2006-01-02")

	queueName := msg.GetQueue()
	keys := []string{
		NewKeyMsgDetail(it.namespace, queueName, msgId),
		NewKeyQueueProcessing(it.namespace, queueName),
		NewKeyQueueFailed(it.namespace, queueName),
		NewKeyDailyStatFailed(it.namespace, queueName, today),
	}

	args := []interface{}{
		msgId,
	}

	reply, err := scriptFail.Run(ctx, it.cli, keys, args...).Result()
	if err != nil {
		if err == redis.ErrClosed {
			return nil
		}

		return err
	}

	rawMsg, err := parseMsgFromRedisLuaHgetallResult(reply)
	if err != nil {
		return err
	}

	rawMsg.State = MsgStateFailed
	rawMsg.Updated = now.UnixMilli()
	rawMsg.Err = errFail.Error()

	keyFailedHis := NewKeyQueueFailedHistory(it.namespace, queueName, msgId)
	dat, _ := json.Marshal(rawMsg)
	_, err = it.cli.LPush(ctx, keyFailedHis, dat).Result()
	if err != nil {
		return err
	}

	_, err = it.cli.LTrim(ctx, keyFailedHis, 0, DefaultMaxItemsLimit-1).Result()
	if err != nil {
		return err
	}

	return nil
}

func parseMsgFromRedisLuaHgetallResult(pairs interface{}) (msg *Msg, err error) {
	arrayOfString, _ := pairs.([]interface{})
	n := len(arrayOfString)
	if n == 0 || n%2 != 0 {
		return nil, ErrInternal
	}

	values := map[string]interface{}{}
	for i := 0; i+2 <= n; i += 2 {
		key, ok := arrayOfString[i].(string)
		if !ok {
			return nil, ErrInternal
		}

		value := arrayOfString[i+1]
		values[key] = value
	}

	payload, _ := values["payload"].(string)
	state, _ := values["state"].(string)
	created, _ := values["created"].(string)
	updated, _ := values["updated"].(string)
	expiredat, _ := values["expiredat"].(string)

	return &Msg{
		Payload:   []byte(payload),
		State:     state,
		Created:   gstr.Atoi64(created),
		Expiredat: gstr.Atoi64(expiredat),
		Updated:   gstr.Atoi64(updated),
	}, nil
}

func NewClientRedis(dsn string) (rs *Client, err error) {
	broker, err := NewBrokerRedis(dsn)
	if err != nil {
		return
	}

	return &Client{broker: broker}, nil
}
