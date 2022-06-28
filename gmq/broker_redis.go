package gmq

import (
	"context"

	"github.com/giant-stone/go/gstr"
	"github.com/giant-stone/go/gtime"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

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
	cli       redis.UniversalClient
	clock     Clock
	namespace string
}

func (it *BrokerRedis) Ping(ctx context.Context) error {
	return it.cli.Ping(ctx).Err()
}

func (it *BrokerRedis) Close() error {
	return it.cli.Close()
}

func (it *BrokerRedis) Init(ctx context.Context, queueName string) (err error) {
	_, err = it.cli.SAdd(ctx, NewKeyQueueList(), queueName).Result()
	return
}

// scriptEnqueue enqueues a message.
//
// Input:
// KEYS[1] -> gmq:<queueName>:msg:<msgId>
// KEYS[2] -> gmq:<queueName>:pending
// --
// ARGV[1] -> <message payload>
// ARGV[2] -> "pending"
// ARGV[3] -> <msgId>
// ARGV[4] -> <current unix time in nsec>
//
// Output:
// Returns 0 if successfully enqueued
// Returns 1 if task ID already exists
var scriptEnqueue = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 1
end
redis.call("HSET", KEYS[1],
           "payload", ARGV[1],
           "state",   ARGV[2],
           "created", ARGV[4])
redis.call("LPUSH", KEYS[2], ARGV[3])
return 0
`)

const (
	LuaReturnCodeSucc = iota
	LuaReturnCodeError
)

func (it *BrokerRedis) Enqueue(ctx context.Context, msg IMsg, opts ...OptionClient) (rs *Msg, err error) {
	payload := msg.GetPayload()
	msgId := msg.GetId()
	if msgId == "" {
		msgId = uuid.NewString()
	}

	queueName := msg.GetQueue()

	for _, opt := range opts {
		switch opt.Type() {
		case OptTypeQueueName:
			{
				value := opt.Value().(string)
				if value != "" {
					queueName = value
				}
			}
		}
	}

	if queueName == "" {
		queueName = DefaultQueueName
	}

	now := it.clock.Now().UnixMilli()
	keys := []string{NewKeyMsgDetail(it.namespace, queueName, msgId), NewKeyQueuePending(it.namespace, queueName)}
	args := []interface{}{
		payload,
		MsgStatePending,
		msgId,
		now,
	}

	resI, err := scriptEnqueue.Run(ctx, it.cli, keys, args...).Result()
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
		Created: now,
		Id:      msgId,
		Payload: payload,
		Queue:   queueName,
		State:   MsgStatePending,
	}, nil
}

// scriptDequeue dequeues a message.
//
// Input:
// KEYS[1] -> gmq:<queueName>:pending
// KEYS[2] -> gmq:<queueName>:paused
// KEYS[3] -> gmq:<queueName>:processing
// --
// ARGV[1] -> message key prefix
//
// Output:
// Returns nil if no processable task is found in the given queue.
// Returns an encoded TaskMessage.
//
var scriptDequeue = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 0 then
	local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
	if id then
		local key = ARGV[1] .. id
		redis.call("HSET", key, "state", "processing")
		redis.call("HSET", key, "processedat", ARGV[2])
		return {id, redis.call("HGETALL", key)}
	end
end
return nil`)

func (it *BrokerRedis) Dequeue(ctx context.Context, queueName string) (msg *Msg, err error) {
	keys := []string{
		NewKeyQueuePending(it.namespace, queueName),
		NewKeyQueuePaused(it.namespace, queueName),
		NewKeyQueueProcessing(it.namespace, queueName),
	}
	args := []interface{}{
		NewKeyMsgDetail(it.namespace, queueName, ""),
		it.clock.Now().UnixMilli(),
	}

	resI, err := scriptDequeue.Run(ctx, it.cli, keys, args...).Result()
	if err != nil {
		if err == redis.Nil {
			err = ErrNoMsg
			return
		}
		err = ErrInternal
		return
	}

	res, ok := resI.([]interface{})
	if !ok {
		// TODO: move this message into internal damaged queue
		err = ErrInternal
		return
	}

	if len(res) != 2 {
		// TODO: move this message into internal damaged queue
		err = ErrInternal
		return
	}

	msgId, _ := res[0].(string)
	arrayOfString, _ := res[1].([]interface{})
	n := len(arrayOfString)
	if msgId == "" || n == 0 || n%2 != 0 {
		// TODO: move this message into internal damaged queue
		err = ErrInternal
		return
	}

	values := map[string]interface{}{}
	for i := 0; i+2 < n; i += 2 {
		key, ok := arrayOfString[i].(string)
		if !ok {
			err = ErrInternal
			return
		}
		value := arrayOfString[i+1]
		values[key] = value
	}

	payload, _ := values["payload"].(string)
	state, _ := values["state"].(string)
	created, _ := values["created"].(string)
	Processedat, _ := values["Processedat"].(string)

	return &Msg{
		Payload:     []byte(payload),
		Id:          msgId,
		Queue:       queueName,
		State:       state,
		Created:     gstr.Atoi64(created),
		Processedat: gstr.Atoi64(Processedat),
	}, nil
}

// scriptDelete delete a message.
//
// KEYS[1] -> gmq:<queueName>:pending
// KEYS[2] -> gmq:<queueName>:processing
// KEYS[3] -> gmq:<queueName>:waiting
// KEYS[4] -> gmq:<queueName>:failed
// KEYS[5] -> gmq:<queueName>:msg:<msgId>
//
// ARGV[1] -> <msgId>
var scriptDelete = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("LREM", KEYS[2], 0, ARGV[1])
redis.call("LREM", KEYS[3], 0, ARGV[1])
redis.call("LREM", KEYS[4], 0, ARGV[1])
if redis.call("DEL", KEYS[5]) == 0 then
	return 1
else
	return 0
end
`)

func (it *BrokerRedis) Delete(ctx context.Context, queueName, msgId string) (err error) {
	keys := []string{
		NewKeyQueuePending(it.namespace, queueName),
		NewKeyQueueProcessing(it.namespace, queueName),
		NewKeyQueueWaiting(it.namespace, queueName),
		NewKeyQueueFailed(it.namespace, queueName),
		NewKeyMsgDetail(it.namespace, queueName, msgId),
	}

	argv := []interface{}{
		msgId,
	}

	resI, err := scriptDelete.Run(ctx, it.cli, keys, argv...).Result()
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

// delete entries old than first entry of pending
func (it *BrokerRedis) DeleteAgo(ctx context.Context, queueName string, seconds int64) (err error) {
	// TBD
	return
}

// scriptComplete marks a message consumed successfully.
//
// KEYS[1] -> gmq:<queueName>:processing
// KEYS[2] -> gmq:<queueName>:t:<msgId>
// KEYS[3] -> gmq:<queueName>:processed:<YYYY-MM-DD>
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

	keys := []string{
		NewKeyQueueProcessing(it.namespace, queueName),
		NewKeyMsgDetail(it.namespace, queueName, msgId),
		NewKeyDailyStatProcessed(it.namespace, queueName, gtime.UnixTime2YyyymmddUtc(it.clock.Now().Unix())),
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

func (it *BrokerRedis) Get(ctx context.Context, queueName, msgId string) (msg *Msg, err error) {
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
		Payload:     []byte(payload),
		Id:          msgId,
		Queue:       queueName,
		State:       values["state"],
		Created:     gstr.Atoi64(values["created"]),
		Processedat: gstr.Atoi64(values["Processed"]),
	}, nil
}

type QueueStat struct {
	Name       string
	Total      int64 // all state of message store in Redis
	Pending    int64 // wait to free worker consume it
	Waiting    int64
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
		waiting, _ := it.cli.LLen(ctx, NewKeyQueueWaiting(it.namespace, queueName)).Result()
		processing, _ := it.cli.LLen(ctx, NewKeyQueueProcessing(it.namespace, queueName)).Result()
		failed, _ := it.cli.LLen(ctx, NewKeyQueueFailed(it.namespace, queueName)).Result()
		total := pending + waiting + processing + failed

		rs = append(rs, &QueueStat{
			Name:       queueName,
			Total:      total,
			Pending:    pending,
			Waiting:    waiting,
			Processing: processing,
			Failed:     failed,
		})
	}
	return
}

type QueueDailyStat struct {
	Date      string // YYYY-MM-DD in UTC
	Processed int64
	Failed    int64
}

func (it *BrokerRedis) GetStatsByDate(ctx context.Context, date string) (rs *QueueDailyStat, err error) {
	queueNames, err := it.listQueues(ctx)
	if err != nil {
		return
	}

	rs = &QueueDailyStat{Date: date}
	for _, queueName := range queueNames {
		processed, _ := it.cli.Get(ctx, NewKeyDailyStatProcessed(it.namespace, queueName, date)).Result()
		failed, _ := it.cli.Get(ctx, NewKeyDailyStatFailed(it.namespace, queueName, date)).Result()

		if processed != "" {
			value := gstr.Atoi64(processed)
			if value > 0 {
				rs.Processed += value
			}
		}

		if failed != "" {
			value := gstr.Atoi64(failed)
			if value > 0 {
				rs.Failed += value
			}
		}
	}

	return
}

func (it *BrokerRedis) Fail(ctx context.Context, msg IMsg, errFail error) (err error) {
	// TBD
	return
}
