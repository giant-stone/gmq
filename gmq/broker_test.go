package gmq_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/grand"
	"github.com/stretchr/testify/require"
)

func testBroker_Enqueue(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	msgGot, err := broker.Enqueue(context.Background(), msgWant)
	require.NoError(t, err)

	now := time.Now().Unix()

	require.Equal(t, msgWant.GetId(), msgGot.Id)
	require.Equal(t, msgWant.GetQueue(), msgGot.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload())
	require.Equal(t, gmq.MsgStatePending, msgGot.State)
	require.Equal(t, now, time.UnixMilli(msgGot.Created).Unix())
	require.Equal(t, msgGot.Expiredat, int64(0))
	require.Equal(t, msgGot.Err, "")
	require.Equal(t, now, time.UnixMilli(msgGot.Updated).Unix())
}

func testBroker_GetMsg(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	msgReturn, err := broker.Enqueue(context.Background(), msgWant)
	require.NoError(t, err)
	gotPayload := msgReturn.GetPayload()
	require.Equal(t, msgWant.GetPayload(), gotPayload)

	msgGot, err := broker.GetMsg(context.Background(), msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)

	now := time.Now().Unix()

	require.Equal(t, msgWant.GetId(), msgGot.Id)
	require.Equal(t, msgWant.GetQueue(), msgGot.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload())
	require.Equal(t, gmq.MsgStatePending, msgGot.State)
	require.Equal(t, now, time.UnixMilli(msgGot.Created).Unix())
	require.Equal(t, msgGot.Expiredat, int64(0))
	require.Equal(t, msgGot.Err, "")
	require.Equal(t, now, time.UnixMilli(msgGot.Updated).Unix())
}

func testBroker_Dequeue(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	id := fmt.Sprintf("%d", time.Now().UnixMilli())
	type Payload struct {
		Fulluri string
		Data    string
	}
	p := Payload{Fulluri: "https://foo.bar", Data: "hello"}
	dat, _ := json.Marshal(p)
	msgWant := &gmq.Msg{Payload: dat, Id: id}

	_, err := broker.Enqueue(context.Background(), msgWant)
	require.NoError(t, err)

	msgGot, err := broker.Dequeue(context.Background(), gmq.DefaultQueueName)
	require.NoError(t, err)

	now := time.Now().Unix()

	require.Equal(t, msgWant.GetId(), msgGot.Id)
	require.Equal(t, gmq.DefaultQueueName, msgGot.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload())
	require.Equal(t, gmq.MsgStateProcessing, msgGot.State)
	require.Equal(t, now, time.UnixMilli(msgGot.Created).Unix())
	require.Equal(t, msgGot.Expiredat, int64(0))
	require.Equal(t, msgGot.Err, "")
	require.Equal(t, now, time.UnixMilli(msgGot.Updated).Unix())
}

func testBroker_DeleteMsg(t *testing.T, broker gmq.Broker) {
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
	require.ErrorIs(t, err, gmq.ErrNoMsg)
}

func testBroker_DeleteQueue(t *testing.T, broker gmq.Broker) {
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
	require.ErrorIs(t, err, gmq.ErrNoMsg)

	_, err = broker.Dequeue(context.Background(), msgWant.GetQueue())
	require.ErrorIs(t, err, gmq.ErrNoMsg)
}

func testBroker_DeleteAgo(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msg := GenerateNewMsg()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()
	clock := gmq.NewSimulatedClock(now)
	broker.SetClock(clock)

	restIfNoMsg := time.Millisecond * time.Duration(10)
	msgMaxTTL := time.Millisecond * time.Duration(40)
	glogging.Init([]string{"stderr"}, "warn")
	srv := gmq.NewServer(ctx, broker, &gmq.Config{MsgMaxTTL: msgMaxTTL, RestIfNoMsg: restIfNoMsg})
	mux := gmq.NewMux()

	mux.Handle(msg.Queue, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
		return errors.New("error")
	}))

	err := srv.Run(mux)
	require.NoError(t, err, "srv.Run")

	_, err = broker.Enqueue(ctx, msg)
	require.NoError(t, err)

	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	// check it first time
	// auto archive it if it failed
	for _, state := range []string{
		gmq.MsgStatePending,
		gmq.MsgStateProcessing,
	} {
		keys, err := broker.ListMsg(ctx, msg.Queue, state, 0, -1)
		require.NoError(t, err)
		require.Empty(t, keys)
	}
	keys, err := broker.ListMsg(ctx, msg.Queue, gmq.MsgStateFailed, 0, -1)
	require.NoError(t, err)
	require.Equal(t, msg.Id, keys[0])
	gotMsg, err := broker.GetMsg(ctx, msg.Queue, msg.Id)
	require.NoError(t, err)
	require.Equal(t, msg.Id, gotMsg.Id)

	queueStats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)
	queueStat := queueStats[0]
	require.Equal(t, msg.Queue, queueStat.Name)
	require.Equal(t, int64(0), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Processing)

	todayYYYYMMDD := time.Now().Format("2006-01-02")
	queueDailyStat, err := broker.GetStatsByDate(ctx, todayYYYYMMDD)
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
		gmq.MsgStatePending,
		gmq.MsgStateProcessing,
		gmq.MsgStateFailed,
	} {
		keys, err := broker.ListMsg(ctx, msg.Queue, state, 0, -1)
		require.NoError(t, err)
		require.Empty(t, keys)
	}

	_, err = broker.GetMsg(ctx, msg.Queue, msg.Id)
	require.ErrorIs(t, gmq.ErrNoMsg, err)

	queueStats, err = broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)
	queueStat = queueStats[0]
	require.Equal(t, msg.Queue, queueStat.Name)
	require.Equal(t, int64(0), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Processing)

	queueDailyStat, err = broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, queueDailyStat.Date)
	require.Equal(t, int64(1), queueDailyStat.Total)
	require.Equal(t, int64(0), queueDailyStat.Completed)
	require.Equal(t, int64(1), queueDailyStat.Failed)
}

func testBroker_Complete(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.Dequeue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	err = broker.Complete(ctx, msgGot)
	require.NoError(t, err)

	_, err = broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.ErrorIs(t, err, gmq.ErrNoMsg)

	_, err = broker.Dequeue(ctx, msgWant.GetQueue())
	require.ErrorIs(t, err, gmq.ErrNoMsg)

	// check queue list
	for _, state := range []string{gmq.MsgStatePending, gmq.MsgStateProcessing, gmq.MsgStateFailed} {
		msgIds, err := broker.ListMsg(ctx, msgWant.Queue, state, 0, -1)
		require.NoError(t, err)
		require.Empty(t, msgIds)
	}

	// check stat
	todayYYYYMMDD := time.Now().Format("2006-01-02")

	queueStats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)
	queueStat := queueStats[0]
	require.Equal(t, msgWant.Queue, queueStat.Name)
	require.Equal(t, int64(0), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Processing)

	queueDailyStat, err := broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, queueDailyStat.Date)
	require.Equal(t, int64(1), queueDailyStat.Total)
	require.Equal(t, int64(1), queueDailyStat.Completed)
	require.Equal(t, int64(0), queueDailyStat.Failed)
}

func testBroker_Fail(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.Dequeue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	errFail := errors.New("something wrong")
	err = broker.Fail(ctx, msgGot, errFail)
	require.NoError(t, err)

	// auto archive fail
	msgGotFailed, err := broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)
	now := time.Now().Unix()
	require.Equal(t, msgWant.GetId(), msgGotFailed.Id)
	require.Equal(t, msgWant.GetQueue(), msgGotFailed.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGotFailed.GetPayload())
	require.Equal(t, gmq.MsgStateFailed, msgGotFailed.State)
	require.Equal(t, now, time.UnixMilli(msgGotFailed.Created).Unix())
	require.Equal(t, msgGotFailed.Expiredat, int64(0))
	require.Equal(t, msgGotFailed.Err, errFail.Error())
	require.Equal(t, now, time.UnixMilli(msgGotFailed.Updated).Unix())

	// check queue list
	_, err = broker.Dequeue(ctx, msgWant.GetQueue())
	require.ErrorIs(t, err, gmq.ErrNoMsg)

	for _, state := range []string{gmq.MsgStatePending, gmq.MsgStateProcessing} {
		msgIds, err := broker.ListMsg(ctx, msgWant.Queue, state, 0, -1)
		require.NoError(t, err)
		require.Empty(t, msgIds)
	}

	// check stat
	todayYYYYMMDD := time.Now().Format("2006-01-02")

	queueStats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, len(queueStats), 1)
	queueStat := queueStats[0]
	require.Equal(t, msgWant.Queue, queueStat.Name)
	require.Equal(t, int64(0), queueStat.Total)
	require.Equal(t, int64(0), queueStat.Pending)
	require.Equal(t, int64(0), queueStat.Processing)

	queueDailyStat, err := broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, queueDailyStat.Date)
	require.Equal(t, int64(1), queueDailyStat.Total)
	require.Equal(t, int64(0), queueDailyStat.Completed)
	require.Equal(t, int64(1), queueDailyStat.Failed)
}

func testBroker_GetStats(t *testing.T, broker gmq.Broker) {
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
	clock := gmq.NewSimulatedClock(now)
	broker.SetClock(clock)

	restIfNoMsg := time.Duration(10) * time.Millisecond
	glogging.Init([]string{"stderr"}, "warn")
	srv := gmq.NewServer(ctx, broker, &gmq.Config{RestIfNoMsg: restIfNoMsg, MsgMaxTTL: time.Minute, Logger: glogging.Sugared})
	mux := gmq.NewMux()
	mux.Handle(msgFail.Queue, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
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
	require.Equal(t, int64(0), qs.Total)
	require.Equal(t, int64(0), qs.Pending)
	require.Equal(t, int64(0), qs.Processing)

	_, err = broker.Enqueue(ctx, msgSucc)
	require.NoError(t, err)
	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	queueStats, err = broker.GetStats(ctx)
	require.NoError(t, err)
	qs = queueStats[0]
	require.Equal(t, int64(0), qs.Total)
	require.Equal(t, int64(0), qs.Pending)
	require.Equal(t, int64(0), qs.Processing)

	_, err = broker.Enqueue(ctx, msgProcessing)
	require.NoError(t, err)
	// wait consumer done
	clock.AdvanceTime(restIfNoMsg * 2)
	time.Sleep(restIfNoMsg * 2)

	queueStats, err = broker.GetStats(ctx)
	require.NoError(t, err)
	qs = queueStats[0]
	require.Equal(t, int64(1), qs.Total)
	require.Equal(t, int64(0), qs.Pending)
	require.Equal(t, int64(1), qs.Processing)
}

func testBroker_GetStatsByDate(t *testing.T, broker gmq.Broker) {
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
	clock := gmq.NewSimulatedClock(now)
	broker.SetClock(clock)

	restIfNoMsg := time.Duration(10) * time.Millisecond
	glogging.Init([]string{"stderr"}, "warn")
	srv := gmq.NewServer(ctx, broker, &gmq.Config{RestIfNoMsg: restIfNoMsg, MsgMaxTTL: time.Minute, Logger: glogging.Sugared})
	mux := gmq.NewMux()
	mux.Handle(msgFail.Queue, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) (err error) {
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

	todayYYYYMMDD := time.Now().Format("2006-01-02")
	rs, err := broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, rs.Date)
	require.Equal(t, int64(2), rs.Total)
	require.Equal(t, int64(1), rs.Completed)
	require.Equal(t, int64(1), rs.Failed)
}
