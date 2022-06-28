package gmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

func TestClient_Enqueue(t *testing.T) {
	broker := setup(t)
	defer broker.Close()

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	now := time.Now()
	nowNano := now.UnixNano()
	msgId := fmt.Sprintf("%d", nowNano)
	queueName := "q" + msgId
	msgWant := &gmq.Msg{
		Payload: []byte(`{"hello":"world"}`),
		Id:      msgId,
		Queue:   queueName,
	}

	// validate message via function return
	msgGot, err := cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err, "Enqueue")
	require.Equal(t, msgWant.GetQueue(), msgGot.GetQueue(), "GetQueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload(), "GetPayload")
	require.Equal(t, gmq.MsgStatePending, msgGot.State, "msg.State")
	require.Equal(t, now.UnixMilli(), msgGot.Created, "msg.Created")
	require.Equal(t, int64(0), msgGot.Processedat, "msg.Processedat")

	// validate message via broker lower API
	msgGot, err = broker.Get(context.Background(), queueName, msgId)
	require.NoError(t, err, "broker.Get")
	require.Equal(t, msgWant.GetQueue(), msgGot.GetQueue(), "GetQueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload(), "GetPayload")
	require.Equal(t, gmq.MsgStatePending, msgGot.State, "msg.State")
	require.Equal(t, now.UnixMilli(), msgGot.Created, "msg.Created")
	require.Equal(t, int64(0), msgGot.Processedat, "msg.Processedat")
}

func TestClient_EnqueueDuplicatedMsg(t *testing.T) {
	broker := setup(t)
	defer broker.Close()

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	now := time.Now().UnixNano()
	msgId := fmt.Sprintf("%d", now)
	msgWant := &gmq.Msg{
		Payload: []byte(`{"hello":"world"}`),
		Id:      msgId,
	}

	rsEnqueue, err := cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err, "Enqueue")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "ErrMsgIdConflict")

	// it could not remove msgId unique constraint via broker.Dequeue
	_, err = broker.Dequeue(context.Background(), rsEnqueue.GetQueue())
	require.NoError(t, err, "Dequeue")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "ErrMsgIdConflict")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "ErrMsgIdConflict")

	// remove msgId unique constraint via broker.Delete
	err = broker.Delete(context.Background(), rsEnqueue.GetQueue(), msgId)
	require.NoError(t, err, "Delete")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err, "Enqueue")
}

func TestClient_EnqueueOptQueueName(t *testing.T) {
	broker := setup(t)
	defer broker.Close()

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	now := time.Now().UnixNano()
	msgId := fmt.Sprintf("%d", now)
	msgWant := &gmq.Msg{
		Payload: []byte(`123`),
		Id:      msgId,
	}
	queueName := "myuniq" + msgId

	// validate message via function return
	msgGot, err := cli.Enqueue(context.Background(), msgWant, gmq.OptQueueName(queueName))
	require.NoError(t, err, "Enqueue")
	require.Equal(t, queueName, msgGot.GetQueue(), "GetQueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")

	// validate message via broker lower API
	msgGot, err = broker.Get(context.Background(), queueName, msgId)
	require.NoError(t, err, "broker.Get")
	require.Equal(t, queueName, msgGot.GetQueue(), "GetQueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")
}

func TestClient_EnqueueOptUniqueIn(t *testing.T) {
	broker := setup(t)
	defer broker.Close()

	now := time.Now()

	clock := gmq.NewSimulatedClock(now)
	broker.SetClock(clock)

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	nowNano := now.UnixNano()
	msgId := fmt.Sprintf("%d", nowNano)
	msgWant := &gmq.Msg{
		Payload: []byte(`123`),
		Id:      msgId,
	}

	uniqIn := time.Millisecond * time.Duration(200)

	msgGot, err := cli.Enqueue(context.Background(), msgWant, gmq.OptUniqueIn(uniqIn))
	require.NoError(t, err, "Enqueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")

	times := 0
	maxTimes := 100
	for times < maxTimes {
		_, err = cli.Enqueue(context.Background(), msgWant, gmq.OptUniqueIn(uniqIn))
		require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "Enqueue with OptUniqueIn")
		times += 1
	}

	// TODO: how to simulated time pass in broker?
	clock.AdvanceTime(uniqIn * 2)
	time.Sleep(uniqIn * 2)

	_, err = cli.Enqueue(context.Background(), msgWant, gmq.OptUniqueIn(uniqIn))
	require.NoError(t, err, "Enqueue with OptUniqueIn")

	_, err = cli.Enqueue(context.Background(), msgWant, gmq.OptUniqueIn(uniqIn))
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "Enqueue with OptUniqueIn")
}
