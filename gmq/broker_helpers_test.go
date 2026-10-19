package gmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/giant-stone/gmq/internal/test_util"
	"github.com/giant-stone/go/grand"
	"github.com/stretchr/testify/require"
)

func generateNewMsg() *Msg {
	queue := grand.String(5)
	id := fmt.Sprintf("%s%s%d", queue, grand.String(5), time.Now().UnixMilli())
	type Payload struct {
		Data string
	}
	p := Payload{Data: grand.String(20)}
	dat, _ := json.Marshal(p)
	return &Msg{Payload: dat, Id: id, Queue: queue}
}

func CheckBrokerEnqueue(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgWant := generateNewMsg()
	msgGot, err := broker.Enqueue(testCtx, msgWant)
	require.NoError(t, err)

	require.Equal(t, msgWant.GetId(), msgGot.Id)
	require.Equal(t, msgWant.GetQueue(), msgGot.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload())
	require.Equal(t, MsgStatePending, msgGot.State)
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Created).Unix())
	require.Equal(t, now.Add(DefaultTTLMsgUniq).Unix(), time.UnixMilli(msgGot.ExpiresAt).Unix())
	require.Equal(t, msgGot.Err, "")
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Updated).Unix())
}

func CheckBrokerGetMsg(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgWant := generateNewMsg()
	msgReturn, err := broker.Enqueue(testCtx, msgWant)
	require.NoError(t, err)
	gotPayload := msgReturn.GetPayload()
	require.Equal(t, msgWant.GetPayload(), gotPayload)

	msgGot, err := broker.GetMsg(testCtx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)

	require.Equal(t, msgWant.GetId(), msgGot.Id)
	require.Equal(t, msgWant.GetQueue(), msgGot.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload())
	require.Equal(t, MsgStatePending, msgGot.State)
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Created).Unix())
	require.Equal(t, now.Add(DefaultTTLMsgUniq).Unix(), time.UnixMilli(msgGot.ExpiresAt).Unix())
	require.Equal(t, msgGot.Err, "")
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Updated).Unix())
}

func CheckBrokerDequeue(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	id := fmt.Sprintf("%d", time.Now().UnixMilli())
	type Payload struct {
		FullURI string
		Data    string
	}
	p := Payload{FullURI: "https://foo.bar", Data: "hello"}
	dat, _ := json.Marshal(p)
	msgWant := &Msg{Payload: dat, Id: id}

	_, err := broker.Enqueue(testCtx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.Dequeue(testCtx, DefaultQueueName)
	require.NoError(t, err)

	require.Equal(t, msgWant.GetId(), msgGot.Id)
	require.Equal(t, DefaultQueueName, msgGot.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload())
	require.Equal(t, MsgStateProcessing, msgGot.State)
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Created).Unix())
	require.Equal(t, now.Add(DefaultTTLMsgUniq).Unix(), time.UnixMilli(msgGot.ExpiresAt).Unix())
	require.Equal(t, msgGot.Err, "")
	require.Equal(t, now.Unix(), time.UnixMilli(msgGot.Updated).Unix())
}

func CheckBrokerDeleteMsg(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := generateNewMsg()
	ctx := testCtx

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

func CheckBrokerDeleteQueue(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := generateNewMsg()
	ctx := testCtx

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)
	require.Equal(t, msgWant.GetId(), msgGot.GetId())

	err = broker.DeleteQueue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	_, err = broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.ErrorIs(t, err, ErrNoMsg)

	_, err = broker.Dequeue(testCtx, msgWant.GetQueue())
	require.ErrorIs(t, err, ErrNoMsg)
}

func CheckBrokerListMsg(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	ctx := testCtx
	queueName := grand.String(10)
	require.NoError(t, broker.Init(ctx, queueName))

	assertState := func(state string, want []string) {
		t.Helper()
		got, err := broker.ListMsg(ctx, queueName, state, DefaultMaxItemsLimit, 0)
		require.NoError(t, err)
		require.Equal(t, want, got, "message state %s", state)
	}

	msgFail := generateNewMsg()
	msgFail.Queue = queueName
	_, err := broker.Enqueue(ctx, msgFail)
	require.NoError(t, err)
	assertState(MsgStatePending, []string{msgFail.Id})
	assertState(MsgStateProcessing, []string{})
	assertState(MsgStateFailed, []string{})

	failed, err := broker.Dequeue(ctx, queueName)
	require.NoError(t, err)
	require.Equal(t, msgFail.Id, failed.Id)
	assertState(MsgStatePending, []string{})
	assertState(MsgStateProcessing, []string{msgFail.Id})
	require.NoError(t, broker.Fail(ctx, failed, errors.New("fail")))
	assertState(MsgStateFailed, []string{msgFail.Id})
	assertState(MsgStatePending, []string{})
	assertState(MsgStateProcessing, []string{})

	msgSuccess := generateNewMsg()
	msgSuccess.Queue = queueName
	_, err = broker.Enqueue(ctx, msgSuccess)
	require.NoError(t, err)
	completed, err := broker.Dequeue(ctx, queueName)
	require.NoError(t, err)
	require.Equal(t, msgSuccess.Id, completed.Id)
	require.NoError(t, broker.Complete(ctx, completed))
	assertState(MsgStateFailed, []string{msgFail.Id})
	assertState(MsgStatePending, []string{})
	assertState(MsgStateProcessing, []string{})

	msgProcessing := generateNewMsg()
	msgProcessing.Queue = queueName
	_, err = broker.Enqueue(ctx, msgProcessing)
	require.NoError(t, err)
	processing, err := broker.Dequeue(ctx, queueName)
	require.NoError(t, err)
	require.Equal(t, msgProcessing.Id, processing.Id)
	assertState(MsgStateFailed, []string{msgFail.Id})
	assertState(MsgStatePending, []string{})
	assertState(MsgStateProcessing, []string{msgProcessing.Id})
}

func CheckBrokerDeleteAgo(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	msg := generateNewMsg()

	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	const msgMaxTTL = 40 * time.Millisecond

	_, err := broker.Enqueue(ctx, msg)
	require.NoError(t, err)
	dequeued, err := broker.Dequeue(ctx, msg.Queue)
	require.NoError(t, err)
	require.NoError(t, broker.Fail(ctx, dequeued, errors.New("handler failed")))

	// Fresh failed messages must survive cleanup.
	require.NoError(t, broker.DeleteAgo(ctx, msg.Queue, msgMaxTTL))

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

	clock.AdvanceTime(msgMaxTTL * 2)
	require.NoError(t, broker.DeleteAgo(ctx, msg.Queue, msgMaxTTL))

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

func CheckBrokerComplete(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgWant := generateNewMsg()
	ctx := testCtx

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

func CheckBrokerFail(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgWant := generateNewMsg()
	ctx := testCtx

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
	require.Equal(t, now.Add(DefaultTTLMsgUniq).Unix(), time.UnixMilli(msgGotFailed.ExpiresAt).Unix())
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

func CheckBrokerListFailed(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	ctx := testCtx

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	queueFoo := grand.String(10)
	groupBar := grand.String(10)

	msgTTLUniq := time.Millisecond * time.Duration(10)

	// mark msg1 fail twice
	msg1 := generateNewMsg()
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
	msg3 := generateNewMsg()
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

func CheckBrokerListFailedMaxItems(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	ctx := testCtx

	now := time.Now()
	clock := NewSimulatedClock(now)
	broker.SetClock(clock)

	msgTTLUniq := time.Millisecond * time.Duration(10)

	msg := generateNewMsg()
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

func CheckBrokerListQueue(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	ctx := testCtx
	msg1 := generateNewMsg()
	broker.Enqueue(ctx, msg1)
	msg2 := generateNewMsg()
	broker.Enqueue(ctx, msg2)
	msg3 := generateNewMsg()
	broker.Enqueue(ctx, msg3)

	m := map[string]struct{}{}
	queues, err := broker.ListQueue(testCtx)
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

func CheckBrokerGetStats(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	msgFail := generateNewMsg()
	msgFail.Payload = []byte(`fail`)

	msgSuccess := generateNewMsg()
	msgSuccess.Queue = msgFail.Queue
	msgSuccess.Payload = []byte(`success`)

	msgProcessing := generateNewMsg()
	msgProcessing.Queue = msgFail.Queue
	msgProcessing.Payload = []byte(`processing`)

	ctx := testCtx
	require.NoError(t, broker.Init(ctx, msgFail.Queue))
	var err error

	_, err = broker.Enqueue(ctx, msgFail)
	require.NoError(t, err)
	dequeued, err := broker.Dequeue(ctx, msgFail.Queue)
	require.NoError(t, err)
	require.Equal(t, msgFail.Id, dequeued.Id)
	require.NoError(t, broker.Fail(ctx, dequeued, errors.New("fail")))

	queueStats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	qs := queueStats[0]
	require.Equal(t, int64(1), qs.Total)
	require.Equal(t, int64(0), qs.Pending)
	require.Equal(t, int64(0), qs.Processing)
	require.Equal(t, int64(1), qs.Failed)

	_, err = broker.Enqueue(ctx, msgSuccess)
	require.NoError(t, err)
	dequeued, err = broker.Dequeue(ctx, msgSuccess.Queue)
	require.NoError(t, err)
	require.Equal(t, msgSuccess.Id, dequeued.Id)
	require.NoError(t, broker.Complete(ctx, dequeued))

	queueStats, err = broker.GetStats(ctx)
	require.NoError(t, err)
	qs = queueStats[0]
	require.Equal(t, int64(1), qs.Total)
	require.Equal(t, int64(0), qs.Pending)
	require.Equal(t, int64(0), qs.Processing)
	require.Equal(t, int64(1), qs.Failed)

	_, err = broker.Enqueue(ctx, msgProcessing)
	require.NoError(t, err)
	dequeued, err = broker.Dequeue(ctx, msgProcessing.Queue)
	require.NoError(t, err)
	require.Equal(t, msgProcessing.Id, dequeued.Id)

	queueStats, err = broker.GetStats(ctx)
	require.NoError(t, err)
	qs = queueStats[0]
	require.Equal(t, int64(2), qs.Total)
	require.Equal(t, int64(0), qs.Pending)
	require.Equal(t, int64(1), qs.Processing)
	require.Equal(t, int64(1), qs.Failed)
}

func CheckBrokerGetStatsByDate(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	queueName := grand.String(10)
	msgFail := generateNewMsg()
	msgFail.Queue = queueName
	msgFail.Payload = []byte(`fail`)

	msgSuccess := generateNewMsg()
	msgSuccess.Queue = queueName
	msgSuccess.Payload = []byte(`success`)

	msgProcessing := generateNewMsg()
	msgProcessing.Queue = queueName
	msgProcessing.Payload = []byte(`processing`)

	ctx := testCtx
	require.NoError(t, broker.Init(ctx, msgFail.Queue))
	var err error

	_, err = broker.Enqueue(ctx, msgFail)
	require.NoError(t, err)
	dequeued, err := broker.Dequeue(ctx, msgFail.Queue)
	require.NoError(t, err)
	require.Equal(t, msgFail.Id, dequeued.Id)
	require.NoError(t, broker.Fail(ctx, dequeued, errors.New("fail")))
	_, err = broker.Enqueue(ctx, msgSuccess)
	require.NoError(t, err)
	dequeued, err = broker.Dequeue(ctx, msgSuccess.Queue)
	require.NoError(t, err)
	require.Equal(t, msgSuccess.Id, dequeued.Id)
	require.NoError(t, broker.Complete(ctx, dequeued))
	_, err = broker.Enqueue(ctx, msgProcessing)
	require.NoError(t, err)
	dequeued, err = broker.Dequeue(ctx, msgProcessing.Queue)
	require.NoError(t, err)
	require.Equal(t, msgProcessing.Id, dequeued.Id)

	listQueueNames, err := broker.ListQueue(ctx)
	require.NoError(t, err)
	todayYYYYMMDD := time.Now().Format("2006-01-02")
	rs, err := broker.GetStatsByDate(ctx, listQueueNames, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, rs.Date)
	require.Equal(t, int64(2), rs.Total)
	require.Equal(t, int64(1), rs.Completed)
	require.Equal(t, int64(1), rs.Failed)
}

func CheckBrokerAutoDeduplicateMsgByDefault(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := generateNewMsg()
	ctx := testCtx

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)
	_, err = broker.Enqueue(ctx, msgWant)
	require.ErrorIs(t, err, ErrMsgIdConflict)

	_, err = broker.Dequeue(ctx, msgWant.Queue)
	require.NoError(t, err)

	// the message Auto Deduplicate if its state is processing
	_, err = broker.Enqueue(ctx, msgWant)
	require.ErrorIs(t, err, ErrMsgIdConflict)
}

func CheckBrokerAutoDeduplicateFailedMsg(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := generateNewMsg()
	ctx := testCtx

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

	// The expired failed message can be retried. Use the default uniqueness
	// window so the following assertions do not depend on Redis round-trip time.
	_, err = broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	_, err = broker.Dequeue(ctx, msgWant.Queue)
	require.NoError(t, err)

	_, err = broker.Enqueue(ctx, msgWant)
	require.ErrorIs(t, err, ErrMsgIdConflict)

	err = broker.DeleteMsg(ctx, msgWant.Queue, msgWant.Id)
	require.NoError(t, err)

	_, err = broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)
	_, err = broker.Enqueue(ctx, msgWant)
	require.ErrorIs(t, err, ErrMsgIdConflict)
}

func CheckBrokerAutoDeduplicateCompletedMsg(t *testing.T, broker Broker) {
	t.Helper()
	testCtx := test_util.Context(t)
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := generateNewMsg()
	ctx := testCtx

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
	require.ErrorIs(t, err, ErrMsgIdConflict)
}
