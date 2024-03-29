package gmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/grand"
	"github.com/stretchr/testify/require"
)

func GenerateNewMsg() *Msg {
	queue := grand.String(5)
	id := fmt.Sprintf("%s%s%d", queue, grand.String(5), time.Now().UnixMilli())
	type Payload struct {
		Data string
	}
	p := Payload{Data: grand.String(20)}
	dat, _ := json.Marshal(p)
	return &Msg{Payload: dat, Id: id, Queue: queue}
}

func TestBroker_Enqueue(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgWant := GenerateNewMsg()
	msgGot, err := broker.Enqueue(context.Background(), msgWant)
	require.NoError(t, err)

	require.Equal(t, msgWant.GetId(), msgGot.Id)
	require.Equal(t, msgWant.GetQueue(), msgGot.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload())
	require.Equal(t, MsgStatePending, msgGot.State)
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Created).Unix())
	require.Equal(t, now.Add(DefaultTTLMsgUniq).Unix(), time.UnixMilli(msgGot.Expireat).Unix())
	require.Equal(t, msgGot.Err, "")
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Updated).Unix())
}

func TestBroker_GetMsg(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgWant := GenerateNewMsg()
	msgReturn, err := broker.Enqueue(context.Background(), msgWant)
	require.NoError(t, err)
	gotPayload := msgReturn.GetPayload()
	require.Equal(t, msgWant.GetPayload(), gotPayload)

	msgGot, err := broker.GetMsg(context.Background(), msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)

	require.Equal(t, msgWant.GetId(), msgGot.Id)
	require.Equal(t, msgWant.GetQueue(), msgGot.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload())
	require.Equal(t, MsgStatePending, msgGot.State)
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Created).Unix())
	require.Equal(t, now.Add(DefaultTTLMsgUniq).Unix(), time.UnixMilli(msgGot.Expireat).Unix())
	require.Equal(t, msgGot.Err, "")
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Updated).Unix())
}

func TestBroker_Dequeue(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	id := fmt.Sprintf("%d", time.Now().UnixMilli())
	type Payload struct {
		Fulluri string
		Data    string
	}
	p := Payload{Fulluri: "https://foo.bar", Data: "hello"}
	dat, _ := json.Marshal(p)
	msgWant := &Msg{Payload: dat, Id: id}

	_, err := broker.Enqueue(context.Background(), msgWant)
	require.NoError(t, err)

	msgGot, err := broker.Dequeue(context.Background(), DefaultQueueName)
	require.NoError(t, err)

	require.Equal(t, msgWant.GetId(), msgGot.Id)
	require.Equal(t, DefaultQueueName, msgGot.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload())
	require.Equal(t, MsgStateProcessing, msgGot.State)
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Created).Unix())
	require.Equal(t, now.Add(DefaultTTLMsgUniq).Unix(), time.UnixMilli(msgGot.Expireat).Unix())
	require.Equal(t, msgGot.Err, "")
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Updated).Unix())
}

func TestBroker_DeleteMsg(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)
	require.Equal(t, msgWant.GetId(), msgGot.GetId())

	err = broker.DeleteMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)

	_, err = broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.ErrorIs(t, err, ErrNoMsg)
}

func TestBroker_DeleteQueue(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)
	require.Equal(t, msgWant.GetId(), msgGot.GetId())

	err = broker.DeleteQueue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	_, err = broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.ErrorIs(t, err, ErrNoMsg)

	_, err = broker.Dequeue(context.Background(), msgWant.GetQueue())
	require.ErrorIs(t, err, ErrNoMsg)
}

func TestBroker_ListMsg(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	queueName := grand.String(10)
	msgFail := GenerateNewMsg()
	msgFail.Queue = queueName
	msgFail.Payload = []byte(`fail`)

	msgSucc := GenerateNewMsg()
	msgSucc.Queue = queueName
	msgSucc.Payload = []byte(`succ`)

	msgProcessing := GenerateNewMsg()
	msgProcessing.Queue = queueName
	msgProcessing.Payload = []byte(`processing`)

	ctx := context.Background()
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	restIfNoMsg := time.Duration(10) * time.Millisecond
	glogging.Init([]string{"stderr"}, "warn")
	srv := NewServer(ctx, broker, &Config{RestIfNoMsg: restIfNoMsg, MsgMaxTTL: time.Minute, Logger: glogging.Sugared})
	mux := NewMux()
	mux.Handle(queueName, HandlerFunc(func(ctx context.Context, msg IMsg) (err error) {
		p := string(msg.GetPayload())
		if p == "fail" {
			return errors.New("fail")
		} else if p == "succ" {
			return nil
		} else if p == "processing" {
			time.Sleep(time.Minute)
		}
		return nil
	}))

	err := srv.Run(mux)
	require.NoError(t, err)

	_, err = broker.Enqueue(ctx, msgFail)
	require.NoError(t, err)
	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	msgIds, _ := broker.ListMsg(ctx, queueName, MsgStateFailed, DefaultMaxItemsLimit, 0)
	require.Equal(t, 1, len(msgIds))
	require.Equal(t, msgFail.Id, msgIds[0])
	msgIds, _ = broker.ListMsg(ctx, queueName, MsgStatePending, DefaultMaxItemsLimit, 0)
	require.Equal(t, 0, len(msgIds))
	msgIds, _ = broker.ListMsg(ctx, queueName, MsgStateProcessing, DefaultMaxItemsLimit, 0)
	require.Equal(t, 0, len(msgIds))

	_, err = broker.Enqueue(ctx, msgSucc)
	require.NoError(t, err)
	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	msgIds, _ = broker.ListMsg(ctx, queueName, MsgStateFailed, DefaultMaxItemsLimit, 0)
	require.Equal(t, 1, len(msgIds))
	require.Equal(t, msgFail.Id, msgIds[0])
	msgIds, _ = broker.ListMsg(ctx, queueName, MsgStatePending, DefaultMaxItemsLimit, 0)
	require.Equal(t, 0, len(msgIds))
	msgIds, _ = broker.ListMsg(ctx, queueName, MsgStateProcessing, DefaultMaxItemsLimit, 0)
	require.Equal(t, 0, len(msgIds))

	_, err = broker.Enqueue(ctx, msgProcessing)
	require.NoError(t, err)
	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	msgIds, _ = broker.ListMsg(ctx, queueName, MsgStateFailed, DefaultMaxItemsLimit, 0)
	require.Equal(t, 1, len(msgIds))
	require.Equal(t, msgFail.Id, msgIds[0])
	msgIds, _ = broker.ListMsg(ctx, queueName, MsgStatePending, DefaultMaxItemsLimit, 0)
	require.Equal(t, 0, len(msgIds))
	msgIds, _ = broker.ListMsg(ctx, queueName, MsgStateProcessing, DefaultMaxItemsLimit, 0)
	require.Equal(t, 1, len(msgIds))
	require.Equal(t, msgProcessing.Id, msgIds[0])
}

func TestBroker_DeleteAgo(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msg := GenerateNewMsg()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	restIfNoMsg := time.Millisecond * time.Duration(10)
	msgMaxTTL := time.Millisecond * time.Duration(40)
	msgTTLUniq := time.Millisecond * time.Duration(10)

	glogging.Init([]string{"stderr"}, "warn")
	srv := NewServer(ctx, broker, &Config{MsgMaxTTL: msgMaxTTL, RestIfNoMsg: restIfNoMsg})
	mux := NewMux()

	mux.Handle(msg.Queue, HandlerFunc(func(ctx context.Context, msg IMsg) (err error) {
		return errors.New("error")
	}))

	err := srv.Run(mux)
	require.NoError(t, err, "srv.Run")

	_, err = broker.Enqueue(ctx, msg, OptUniqueIn(msgTTLUniq))
	require.NoError(t, err)

	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	// check it first time
	// auto archive it if it failed
	for _, state := range []string{
		MsgStatePending,
		MsgStateProcessing,
	} {
		keys, err := broker.ListMsg(ctx, msg.Queue, state, 0, 0)
		require.NoError(t, err)
		require.Empty(t, keys)
	}
	keys, err := broker.ListMsg(ctx, msg.Queue, MsgStateFailed, 0, 0)
	require.NoError(t, err)
	require.Equal(t, msg.Id, keys[0])

	_, err = broker.GetMsg(ctx, msg.Queue, msg.Id)
	require.ErrorIs(t, ErrNoMsg, err)

	queueStats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)
	queueStat := queueStats[0]
	require.Equal(t, msg.Queue, queueStat.Name)
	require.Equal(t, int64(1), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Processing)
	require.Equal(t, int64(1), queueStat.Failed)

	listQueueNames := toListQueueNames(queueStats)
	todayYYYYMMDD := time.Now().Format("2006-01-02")
	queueDailyStat, err := broker.GetStatsByDate(ctx, listQueueNames, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, queueDailyStat.Date)
	require.Equal(t, int64(1), queueDailyStat.Total)
	require.Equal(t, int64(0), queueDailyStat.Completed)
	require.Equal(t, int64(1), queueDailyStat.Failed)

	// wait cleaner done
	clock.AdvanceTime(msgMaxTTL * 2)
	time.Sleep(msgMaxTTL * 2)

	// check it second time
	for _, state := range []string{
		MsgStatePending,
		MsgStateProcessing,
		MsgStateFailed,
	} {
		keys, err := broker.ListMsg(ctx, msg.Queue, state, 0, 0)
		require.NoError(t, err)
		require.Empty(t, keys, "state=%s", state)
	}

	_, err = broker.GetMsg(ctx, msg.Queue, msg.Id)
	require.ErrorIs(t, ErrNoMsg, err)

	queueStats, err = broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)
	queueStat = queueStats[0]
	require.Equal(t, msg.Queue, queueStat.Name)
	require.Equal(t, int64(0), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Processing)
	require.Equal(t, int64(0), queueStat.Failed)

	queueDailyStat, err = broker.GetStatsByDate(ctx, listQueueNames, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, queueDailyStat.Date)
	require.Equal(t, int64(1), queueDailyStat.Total)
	require.Equal(t, int64(0), queueDailyStat.Completed)
	require.Equal(t, int64(1), queueDailyStat.Failed)
}

func toListQueueNames(queueStats []*QueueStat) []string {
	rs := make([]string, 0)
	for _, item := range queueStats {
		rs = append(rs, item.Name)
	}
	return rs
}

func TestBroker_Complete(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.Dequeue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	err = broker.Complete(ctx, msgGot)
	require.NoError(t, err)

	_, err = broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.ErrorIs(t, err, ErrNoMsg)

	_, err = broker.Dequeue(ctx, msgWant.GetQueue())
	require.ErrorIs(t, err, ErrNoMsg)

	// check queue list
	for _, state := range []string{MsgStatePending, MsgStateProcessing, MsgStateFailed} {
		msgIds, err := broker.ListMsg(ctx, msgWant.Queue, state, 0, 0)
		require.NoError(t, err)
		require.Empty(t, msgIds)
	}

	// check stat
	todayYYYYMMDD := now.Format("2006-01-02")

	queueStats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)
	queueStat := queueStats[0]
	require.Equal(t, msgWant.Queue, queueStat.Name)
	require.Equal(t, int64(0), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Processing)
	require.Equal(t, int64(0), queueStat.Failed)

	queueDailyStat, err := broker.GetStatsByDate(ctx, toListQueueNames(queueStats), todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, queueDailyStat.Date)
	require.Equal(t, int64(1), queueDailyStat.Total)
	require.Equal(t, int64(1), queueDailyStat.Completed)
	require.Equal(t, int64(0), queueDailyStat.Failed)
}

func TestBroker_Fail(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.Dequeue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	errFail := errors.New("something wrong")
	err = broker.Fail(ctx, msgGot, errFail)
	require.NoError(t, err)

	_, err = broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.ErrorIs(t, ErrNoMsg, err)

	// auto archive failed
	msgs, err := broker.ListFailed(ctx, msgWant.Queue, msgWant.Id, 10, 0)
	require.NoError(t, err)
	msgGotFailed := msgs[0]
	require.Equal(t, msgWant.GetId(), msgGotFailed.Id)
	require.Equal(t, msgWant.GetQueue(), msgGotFailed.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGotFailed.GetPayload())
	require.Equal(t, MsgStateFailed, msgGotFailed.State)
	require.Equal(t, now.Unix(), time.UnixMilli(msgGotFailed.Created).Unix())
	require.Equal(t, now.Add(DefaultTTLMsgUniq).Unix(), time.UnixMilli(msgGotFailed.Expireat).Unix())
	require.Equal(t, msgGotFailed.Err, errFail.Error())
	require.Equal(t, now.Unix(), time.UnixMilli(msgGotFailed.Updated).Unix())

	// check queue list
	_, err = broker.Dequeue(ctx, msgWant.GetQueue())
	require.ErrorIs(t, err, ErrNoMsg)

	for _, state := range []string{MsgStatePending, MsgStateProcessing} {
		msgIds, err := broker.ListMsg(ctx, msgWant.Queue, state, 0, 0)
		require.NoError(t, err)
		require.Empty(t, msgIds)
	}
	msgIds, err := broker.ListMsg(ctx, msgWant.Queue, MsgStateFailed, 0, 0)
	require.NoError(t, err)
	require.Equal(t, msgWant.Id, msgIds[0])

	// check stat
	todayYYYYMMDD := time.Now().Format("2006-01-02")

	queueStats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)
	queueStat := queueStats[0]
	require.Equal(t, msgWant.Queue, queueStat.Name)
	require.Equal(t, int64(1), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Processing)
	require.Equal(t, int64(1), queueStat.Failed)

	queueDailyStat, err := broker.GetStatsByDate(ctx, toListQueueNames(queueStats), todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, queueDailyStat.Date)
	require.Equal(t, int64(1), queueDailyStat.Total)
	require.Equal(t, int64(0), queueDailyStat.Completed)
	require.Equal(t, int64(1), queueDailyStat.Failed)
}

func TestBroker_ListFailed(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	ctx := context.Background()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	queueFoo := grand.String(10)
	groupBar := grand.String(10)

	msgTTLUniq := time.Millisecond * time.Duration(10)

	// mark msg1 fail twice
	msg1 := GenerateNewMsg()
	msg1.Queue = queueFoo
	_, err := broker.Enqueue(ctx, msg1, OptUniqueIn(msgTTLUniq))
	require.NoError(t, err)
	_, err = broker.Dequeue(ctx, msg1.Queue)
	require.NoError(t, err)
	msg1Err := fmt.Errorf("error %s, queueName=%s msgId=%s", grand.String(5), msg1.Queue, msg1.Id)
	err = broker.Fail(ctx, msg1, msg1Err)
	require.NoError(t, err)

	clock.AdvanceTime(msgTTLUniq * 2)
	time.Sleep(msgTTLUniq * 2)

	_, err = broker.Enqueue(ctx, msg1)
	require.NoError(t, err)
	_, err = broker.Dequeue(ctx, msg1.Queue)
	require.NoError(t, err)
	msg2Err := fmt.Errorf("error %s, queueName=%s msgId=%s", grand.String(5), msg1.Queue, msg1.Id)
	err = broker.Fail(ctx, msg1, msg2Err)
	require.NoError(t, err)

	// mark msg3 fail once
	msg3 := GenerateNewMsg()
	msg3.Queue = groupBar
	_, err = broker.Enqueue(ctx, msg3)
	require.NoError(t, err)
	_, err = broker.Dequeue(ctx, msg3.Queue)
	require.NoError(t, err)
	msg3Err := fmt.Errorf("error %s, queueName=%s msgId=%s", grand.String(5), msg3.Queue, msg3.Id)
	err = broker.Fail(ctx, msg3, msg3Err)
	require.NoError(t, err)

	msgs, err := broker.ListFailed(ctx, msg1.Queue, msg1.Id, 10, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(msgs))
	// NOTICE it is order in fresh to old
	msg1Got := msgs[0]
	msg2Got := msgs[1]
	require.Equal(t, msg1.Id, msg1Got.Id)
	require.Equal(t, msg2Err.Error(), msg1Got.Err)
	require.Equal(t, msg1.Id, msg2Got.Id)
	require.Equal(t, msg1Err.Error(), msg2Got.Err)

	msgs, err = broker.ListFailed(ctx, msg3.Queue, msg3.Id, 10, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgs))
	msg3Got := msgs[0]
	require.Equal(t, msg3.Id, msg3Got.Id)
	require.Equal(t, msg3Err.Error(), msg3Got.Err)
}

func TestBroker_ListFailedMaxItems(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	ctx := context.Background()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgTTLUniq := time.Millisecond * time.Duration(10)

	msg := GenerateNewMsg()
	failErrs := []string{}
	for i := 0; i < DefaultMaxItemsLimit*2; i += 1 {
		_, err := broker.Enqueue(ctx, msg, OptUniqueIn(msgTTLUniq))
		require.NoError(t, err)
		_, err = broker.Dequeue(ctx, msg.Queue)
		require.NoError(t, err)
		failErr := fmt.Errorf("error %s, queueName=%s msgId=%s", grand.String(5), msg.Queue, msg.Id)
		failErrs = append(failErrs, failErr.Error())
		err = broker.Fail(ctx, msg, failErr)
		require.NoError(t, err)

		clock.AdvanceTime(msgTTLUniq * 2)
		time.Sleep(msgTTLUniq * 2)
	}

	msgs, err := broker.ListFailed(ctx, msg.Queue, msg.Id, int64(DefaultMaxItemsLimit*2), 0)
	require.NoError(t, err)
	require.Equal(t, DefaultMaxItemsLimit, len(msgs))

	for i := len(msgs); i > 0; i -= 1 {
		require.Equal(t, failErrs[DefaultMaxItemsLimit+i-1], msgs[DefaultMaxItemsLimit-i].Err)
	}
}

func TestBroker_ListQueue(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	ctx := context.Background()
	msg1 := GenerateNewMsg()
	broker.Enqueue(ctx, msg1)
	msg2 := GenerateNewMsg()
	broker.Enqueue(ctx, msg2)
	msg3 := GenerateNewMsg()
	broker.Enqueue(ctx, msg3)

	m := map[string]struct{}{}
	queues, err := broker.ListQueue(context.Background())
	require.NoError(t, err)
	for _, item := range queues {
		m[item] = struct{}{}
	}
	for _, name := range []string{
		msg1.Queue,
		msg2.Queue,
		msg3.Queue,
	} {
		_, ok := m[name]
		require.True(t, ok)
	}
}

func TestBroker_GetStats(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgFail := GenerateNewMsg()
	msgFail.Payload = []byte(`fail`)

	msgSucc := GenerateNewMsg()
	msgSucc.Queue = msgFail.Queue
	msgSucc.Payload = []byte(`succ`)

	msgProcessing := GenerateNewMsg()
	msgProcessing.Queue = msgFail.Queue
	msgProcessing.Payload = []byte(`processing`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	restIfNoMsg := time.Duration(10) * time.Millisecond
	glogging.Init([]string{"stderr"}, "warn")
	srv := NewServer(ctx, broker, &Config{RestIfNoMsg: restIfNoMsg, MsgMaxTTL: time.Minute, Logger: glogging.Sugared})
	mux := NewMux()
	mux.Handle(msgFail.Queue, HandlerFunc(func(ctx context.Context, msg IMsg) (err error) {
		p := string(msg.GetPayload())
		if p == "fail" {
			return errors.New("fail")
		} else if p == "succ" {
			return nil
		} else if p == "processing" {
			time.Sleep(time.Minute)
		}
		return nil
	}))

	err := srv.Run(mux)
	require.NoError(t, err, "srv.Run")

	_, err = broker.Enqueue(ctx, msgFail)
	require.NoError(t, err)
	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	queueStats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	qs := queueStats[0]
	require.Equal(t, int64(1), qs.Total)
	require.Equal(t, int64(0), qs.Pending)
	require.Equal(t, int64(0), qs.Processing)
	require.Equal(t, int64(1), qs.Failed)

	_, err = broker.Enqueue(ctx, msgSucc)
	require.NoError(t, err)
	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	queueStats, err = broker.GetStats(ctx)
	require.NoError(t, err)
	qs = queueStats[0]
	require.Equal(t, int64(1), qs.Total)
	require.Equal(t, int64(0), qs.Pending)
	require.Equal(t, int64(0), qs.Processing)
	require.Equal(t, int64(1), qs.Failed)

	_, err = broker.Enqueue(ctx, msgProcessing)
	require.NoError(t, err)
	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	queueStats, err = broker.GetStats(ctx)
	require.NoError(t, err)
	qs = queueStats[0]
	require.Equal(t, int64(2), qs.Total)
	require.Equal(t, int64(0), qs.Pending)
	require.Equal(t, int64(1), qs.Processing)
	require.Equal(t, int64(1), qs.Failed)
}

func TestBroker_GetStatsByDate(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	queueName := grand.String(10)
	msgFail := GenerateNewMsg()
	msgFail.Queue = queueName
	msgFail.Payload = []byte(`fail`)

	msgSucc := GenerateNewMsg()
	msgSucc.Queue = queueName
	msgSucc.Payload = []byte(`succ`)

	msgProcessing := GenerateNewMsg()
	msgProcessing.Queue = queueName
	msgProcessing.Payload = []byte(`processing`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	restIfNoMsg := time.Duration(10) * time.Millisecond
	glogging.Init([]string{"stderr"}, "warn")
	srv := NewServer(ctx, broker, &Config{RestIfNoMsg: restIfNoMsg, MsgMaxTTL: time.Minute, Logger: glogging.Sugared})
	mux := NewMux()
	mux.Handle(msgFail.Queue, HandlerFunc(func(ctx context.Context, msg IMsg) (err error) {
		p := string(msg.GetPayload())
		if p == "fail" {
			return errors.New("fail")
		} else if p == "succ" {
			return nil
		} else if p == "processing" {
			time.Sleep(time.Minute)
		}
		return nil
	}))

	err := srv.Run(mux)
	require.NoError(t, err, "srv.Run")

	_, err = broker.Enqueue(ctx, msgFail)
	require.NoError(t, err)
	_, err = broker.Enqueue(ctx, msgSucc)
	require.NoError(t, err)
	_, err = broker.Enqueue(ctx, msgProcessing)
	require.NoError(t, err)

	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	listQueueNames, _ := broker.ListQueue(ctx)
	todayYYYYMMDD := time.Now().Format("2006-01-02")
	rs, err := broker.GetStatsByDate(ctx, listQueueNames, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, rs.Date)
	require.Equal(t, int64(2), rs.Total)
	require.Equal(t, int64(1), rs.Completed)
	require.Equal(t, int64(1), rs.Failed)
}

func TestBroker_AutoDeduplicateMsgByDefault(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)
	_, err = broker.Enqueue(ctx, msgWant)
	require.ErrorIs(t, ErrMsgIdConflict, err)

	_, err = broker.Dequeue(ctx, msgWant.Queue)
	require.NoError(t, err)

	// the message Auto Deduplicate if its state is processing
	_, err = broker.Enqueue(ctx, msgWant)
	require.ErrorIs(t, ErrMsgIdConflict, err)
}

func TestBroker_AutoDeduplicateFailedMsg(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgTTLUniq := time.Millisecond * time.Duration(10)

	_, err := broker.Enqueue(ctx, msgWant, OptUniqueIn(msgTTLUniq))
	require.NoError(t, err)
	err = broker.Fail(ctx, msgWant, errors.New("something wrong"))
	require.NoError(t, err)

	clock.AdvanceTime(msgTTLUniq * 2)
	time.Sleep(msgTTLUniq * 2)

	// it does not Auto Deduplicate if the message state is not in {pending,processing}
	_, err = broker.Enqueue(ctx, msgWant, OptUniqueIn(msgTTLUniq))
	require.NoError(t, err)

	_, err = broker.Dequeue(ctx, msgWant.Queue)
	require.NoError(t, err)

	_, err = broker.Enqueue(ctx, msgWant)
	require.ErrorIs(t, ErrMsgIdConflict, err)

	err = broker.DeleteMsg(ctx, msgWant.Queue, msgWant.Id)
	require.NoError(t, err)

	_, err = broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)
	_, err = broker.Enqueue(ctx, msgWant)
	require.ErrorIs(t, ErrMsgIdConflict, err)
}

func TestBroker_AutoDeduplicateCompletedMsg(t *testing.T, broker Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgTTLUniq := time.Millisecond * time.Duration(10)

	_, err := broker.Enqueue(ctx, msgWant, OptUniqueIn(msgTTLUniq))
	require.NoError(t, err)
	_, err = broker.Dequeue(ctx, msgWant.Queue)
	require.NoError(t, err)
	err = broker.Complete(ctx, msgWant)
	require.NoError(t, err)

	clock.AdvanceTime(msgTTLUniq * 2)
	time.Sleep(msgTTLUniq * 2)

	_, err = broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	_, err = broker.Enqueue(ctx, msgWant)
	require.ErrorIs(t, ErrMsgIdConflict, err)
}
