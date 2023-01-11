package gmq_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/grand"
	"github.com/giant-stone/go/gtime"
)

var (
	universalBrokerInMemory gmq.Broker
)

func setupBrokerInMemory(t testing.TB) gmq.Broker {
	broker, err := gmq.NewBrokerInMemory(&gmq.BrokerInMemoryOpts{
		MaxBytes: 1024 * 1024,
	})
	require.NoError(t, err)
	universalBrokerInMemory = broker
	return universalBrokerInMemory
}

func getTestBrokerInMemory(t testing.TB) gmq.Broker {
	return setupBrokerInMemory(t)
}

func getTestClientInMemory(t *testing.T) *gmq.Client {
	broker := getTestBrokerInMemory(t)
	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err)
	return cli
}

func TestBrokerInMemory_Enqueue(t *testing.T) {
	cli := getTestClientInMemory(t)
	defer cli.Close()

	msgWant := GenerateNewMsg()
	msgGot, err := cli.Enqueue(context.Background(), msgWant)
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

func TestBrokerInMemory_GetMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	rsEnqueue, err := cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err)
	gotPayload := rsEnqueue.GetPayload()
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

func TestBrokerInMemory_Dequeue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err)
	defer broker.Close()

	id := fmt.Sprintf("%d", time.Now().UnixMilli())
	type Payload struct {
		Fulluri string
		Data    string
	}
	p := Payload{Fulluri: "https://foo.bar", Data: "hello"}
	dat, _ := json.Marshal(p)
	msgWant := &gmq.Msg{Payload: dat, Id: id}

	_, err = cli.Enqueue(context.Background(), msgWant)
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

func TestBrokerInMemory_DeleteMsg(t *testing.T) {
	broker := getTestBrokerInMemory(t)
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

func TestBrokerInMemory_DeleteQueue(t *testing.T) {
	broker := getTestBrokerInMemory(t)
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
}

func TestBrokerInMemory_DeleteAgo(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	defer broker.Close()

	queueName := grand.String(6)

	msgWantShortLife := GenerateNewMsg()
	msgWantShortLife.Queue = queueName

	msgWantLongLive := GenerateNewMsg()
	msgWantLongLive.Queue = queueName

	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWantShortLife)
	require.NoError(t, err)

	msgGot, err := broker.GetMsg(ctx, queueName, msgWantShortLife.GetId())
	require.NoError(t, err)
	require.Equal(t, msgWantShortLife.GetId(), msgGot.GetId())

	now := time.Now().Add(time.Minute * time.Duration(1))
	broker.SetClock(gmq.Clock(gmq.NewSimulatedClock(now)))

	_, err = broker.Enqueue(ctx, msgWantLongLive)
	require.NoError(t, err)

	err = broker.DeleteAgo(ctx, queueName, time.Minute*time.Duration(1))
	require.NoError(t, err)

	_, err = broker.GetMsg(ctx, queueName, msgWantShortLife.GetId())
	require.ErrorIs(t, gmq.ErrNoMsg, err)

	_, err = broker.GetMsg(ctx, queueName, msgWantLongLive.GetId())
	require.NoError(t, err)
}

func TestBrokerInMemory_Complete(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.Dequeue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	err = broker.Complete(ctx, msgGot)
	require.NoError(t, err)

	msgGotCompleted, err := broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)

	now := time.Now()
	nowInUnix := now.Unix()

	require.Equal(t, msgWant.GetId(), msgGotCompleted.Id)
	require.Equal(t, msgWant.GetQueue(), msgGotCompleted.Queue)
	require.Equal(t, msgWant.GetPayload(), msgGotCompleted.GetPayload())
	require.Equal(t, gmq.MsgStateCompleted, msgGotCompleted.State)
	require.Equal(t, nowInUnix, time.UnixMilli(msgGotCompleted.Created).Unix())
	require.Equal(t, msgGotCompleted.Expiredat, int64(0))
	require.Equal(t, msgGotCompleted.Err, "")
	require.Equal(t, nowInUnix, time.UnixMilli(msgGotCompleted.Updated).Unix())

	_, err = broker.Dequeue(ctx, msgWant.GetQueue())
	require.ErrorIs(t, err, gmq.ErrNoMsg)
}

func TestBrokerInMemory_Fail(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()

	_, err := broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	msgGot, err := broker.Dequeue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	errFail := errors.New("somtething wrong")
	err = broker.Fail(ctx, msgGot, errFail)
	require.NoError(t, err)

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

	_, err = broker.Dequeue(ctx, msgWant.GetQueue())
	require.ErrorIs(t, err, gmq.ErrNoMsg)
}

func TestBrokerInMemory_GetStats(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()
	var err error

	_, err = broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	rs, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rs))
	stat := rs[0]
	require.Equal(t, int64(1), stat.Pending)
	require.Zero(t, stat.Waiting)
	require.Zero(t, stat.Processing)
	require.Zero(t, stat.Completed)
	require.Zero(t, stat.Failed)
	require.Equal(t, int64(1), stat.Total)

	msgGot, err := broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)

	rs, err = broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rs))
	stat = rs[0]
	require.Equal(t, int64(1), stat.Pending)
	require.Zero(t, stat.Waiting)
	require.Zero(t, stat.Processing)
	require.Zero(t, stat.Completed)
	require.Zero(t, stat.Failed)
	require.Equal(t, int64(1), stat.Total)

	_, err = broker.Dequeue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	rs, err = broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rs))
	stat = rs[0]
	require.Zero(t, stat.Pending)
	require.Zero(t, stat.Waiting)
	require.Equal(t, int64(1), stat.Processing)
	require.Zero(t, stat.Completed)
	require.Zero(t, stat.Failed)
	require.Equal(t, int64(1), stat.Total)

	_, err = broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)

	rs, err = broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rs))
	stat = rs[0]
	require.Zero(t, stat.Pending)
	require.Zero(t, stat.Waiting)
	require.Equal(t, int64(1), stat.Processing)
	require.Zero(t, stat.Completed)
	require.Zero(t, stat.Failed)
	require.Equal(t, int64(1), stat.Total)

	err = broker.Complete(ctx, msgGot)
	require.NoError(t, err)

	rs, err = broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rs))
	stat = rs[0]
	require.Zero(t, stat.Pending)
	require.Zero(t, stat.Waiting)
	require.Zero(t, stat.Processing)
	require.Equal(t, int64(1), stat.Completed)
	require.Zero(t, stat.Failed)
	require.Equal(t, int64(1), stat.Total)

	_, err = broker.GetMsg(ctx, msgWant.GetQueue(), msgWant.GetId())
	require.NoError(t, err)

	rs, err = broker.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rs))
	stat = rs[0]
	require.Zero(t, stat.Pending)
	require.Zero(t, stat.Waiting)
	require.Zero(t, stat.Processing)
	require.Equal(t, int64(1), stat.Completed)
	require.Zero(t, stat.Failed)
	require.Equal(t, int64(1), stat.Total)
}

func TestBrokerInMemory_GetStatsByDate(t *testing.T) {
	broker := getTestBrokerInMemory(t)
	defer broker.Close()

	msgWant := GenerateNewMsg()
	ctx := context.Background()
	var err error

	_, err = broker.Enqueue(ctx, msgWant)
	require.NoError(t, err)

	todayYYYYMMDD := gtime.UnixTime2YyyymmddUtc(time.Now().Unix())
	rs, err := broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, rs.Date)
	require.Zero(t, rs.Completed)
	require.Zero(t, rs.Failed)

	_, err = broker.Dequeue(ctx, msgWant.GetQueue())
	require.NoError(t, err)

	rs, err = broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, rs.Date)
	require.Zero(t, rs.Completed)
	require.Zero(t, rs.Failed)

	err = broker.Complete(ctx, msgWant)
	require.NoError(t, err)

	rs, err = broker.GetStatsByDate(ctx, todayYYYYMMDD)
	require.NoError(t, err)
	require.Equal(t, todayYYYYMMDD, rs.Date)
	require.Equal(t, int64(1), rs.Completed)
	require.Zero(t, rs.Failed)
}
