package gmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/grand"
)

func testClient_Enqueue(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	now := time.Now()
	msgId := fmt.Sprintf("%s.%d", grand.String(10), now.UnixNano())
	queueName := grand.String(10)
	payload := []byte(`{"hello":"world"}`)
	msgWant := &gmq.Msg{
		Payload: payload,
		Id:      msgId,
		Queue:   queueName,
	}

	// validate message via function return
	msgGot, err := cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err, "Enqueue")
	require.Equal(t, msgWant.GetQueue(), msgGot.GetQueue(), "GetQueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload(), "GetPayload")
	require.Equal(t, payload, msgGot.GetPayload(), "GetPayload")
	require.Equal(t, gmq.MsgStatePending, msgGot.State, "msg.State")
	require.Equal(t, now.UnixMilli()/1000, msgGot.Created/1000, "msg.Created")
	require.Equal(t, now.UnixMilli()/1000, msgGot.Updated/1000, "msg.Updated")

	// validate message via broker lower API
	msgGot, err = broker.GetMsg(context.Background(), queueName, msgId)
	require.NoError(t, err, "broker.GetMsg")
	require.Equal(t, msgWant.GetQueue(), msgGot.GetQueue(), "GetQueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload(), "GetPayload")
	require.Equal(t, payload, msgGot.GetPayload(), "GetPayload")
	require.Equal(t, gmq.MsgStatePending, msgGot.State, "msg.State")
	require.Equal(t, now.UnixMilli()/1000, msgGot.Created/1000, "msg.Created")
	require.Equal(t, now.UnixMilli()/1000, msgGot.Updated/1000, "msg.Updated")
}

func testClient_Dequeue(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	now := time.Now()
	nowMilli := now.UnixMilli()
	msgId := fmt.Sprintf("%s.%d", grand.String(10), now.UnixNano())
	payload := []byte(`{"hello":"world"}`)
	queueName := grand.String(10)
	msgWant := &gmq.Msg{
		Payload: payload,
		Id:      msgId,
		Queue:   queueName,
	}

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err, "Enqueue")

	msgGot, err := broker.Dequeue(context.Background(), queueName)
	require.NoError(t, err, "Dequeue")
	require.Equal(t, msgWant.GetQueue(), msgGot.GetQueue(), "GetQueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload(), "GetPayload")
	require.Equal(t, payload, msgGot.GetPayload(), "GetPayload")
	require.Equal(t, gmq.MsgStateProcessing, msgGot.State, "msg.State")
	require.Equal(t, nowMilli/1000, msgGot.Created/1000, "msg.Created")
	require.LessOrEqual(t, nowMilli, msgGot.Updated, "msg.Updated")
}

func testClient_EnqueueDuplicatedMsg(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	now := time.Now()
	msgId := fmt.Sprintf("%s.%d", grand.String(10), now.UnixNano())
	payload := []byte(`{"hello":"world"}`)
	queueName := grand.String(10)
	msgWant := &gmq.Msg{
		Payload: payload,
		Id:      msgId,
		Queue:   queueName,
	}

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err, "Enqueue")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "ErrMsgIdConflict")

	// it could not remove msgId unique constraint via broker.Dequeue
	_, err = broker.Dequeue(context.Background(), queueName)
	require.NoError(t, err, "Dequeue")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "ErrMsgIdConflict")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "ErrMsgIdConflict")

	// remove msgId unique constraint via broker.DeleteMsg
	err = broker.DeleteMsg(context.Background(), queueName, msgId)
	require.NoError(t, err, "Delete")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err, "Enqueue")
}

func testClient_EnqueueOptQueueName(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	now := time.Now()
	msgId := fmt.Sprintf("%s.%d", grand.String(10), now.UnixNano())
	payload := []byte(`{"hello":"world"}`)
	queueName := grand.String(10)
	msgWant := &gmq.Msg{
		Payload: payload,
		Id:      msgId,
		Queue:   queueName,
	}

	// validate message via function return
	msgGot, err := cli.Enqueue(context.Background(), msgWant, gmq.OptQueueName(queueName))
	require.NoError(t, err, "Enqueue")
	require.Equal(t, queueName, msgGot.GetQueue(), "GetQueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload(), "GetPayload")
	require.Equal(t, payload, msgGot.GetPayload(), "GetPayload")

	// validate message via broker lower API
	msgGot, err = broker.GetMsg(context.Background(), queueName, msgId)
	require.NoError(t, err, "broker.GetMsg")
	require.Equal(t, queueName, msgGot.GetQueue(), "GetQueue")
	require.Equal(t, payload, msgGot.GetPayload(), "GetPayload")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")
}

func testClient_EnqueueOptUniqueIn(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	now := time.Now()

	clock := gmq.NewSimulatedClock(now)
	broker.SetClock(clock)

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	msgId := fmt.Sprintf("%s.%d", grand.String(10), now.UnixNano())
	payload := []byte(`{"hello":"world"}`)
	queueName := grand.String(10)
	msgWant := &gmq.Msg{
		Payload: payload,
		Id:      msgId,
		Queue:   queueName,
	}

	uniqIn := time.Second * time.Duration(1)

	msgGot, err := cli.Enqueue(context.Background(), msgWant, gmq.OptUniqueIn(uniqIn))
	require.NoError(t, err, "Enqueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")

	times := 0
	maxTimes := 10
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

func testClient_EnqueueOptTypeIgnoreUnique(t *testing.T, broker gmq.Broker) {
	require.NotNil(t, broker)
	defer broker.Close()

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err, "gmq.NewClientFromBroker")

	now := time.Now()
	msgId := fmt.Sprintf("%s.%d", grand.String(10), now.UnixNano())
	payload := []byte(`{"hello":"world"}`)
	queueName := grand.String(10)
	msgWant := &gmq.Msg{
		Payload: payload,
		Id:      msgId,
		Queue:   queueName,
	}

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err, "Enqueue")

	_, err = cli.Enqueue(context.Background(), msgWant, gmq.OptIgnoreUnique(true))
	require.NoError(t, err, "OptIgnoreUnique")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "ErrMsgIdConflict")

	// it could not remove msgId unique constraint via broker.Dequeue
	_, err = broker.Dequeue(context.Background(), queueName)
	require.NoError(t, err, "Dequeue")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "ErrMsgIdConflict")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.ErrorIs(t, err, gmq.ErrMsgIdConflict, "ErrMsgIdConflict")

	// remove msgId unique constraint via broker.DeleteMsg
	err = broker.DeleteMsg(context.Background(), queueName, msgId)
	require.NoError(t, err, "Delete")

	_, err = cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err, "Enqueue")

	_, err = cli.Enqueue(context.Background(), msgWant, gmq.OptIgnoreUnique(true))
	require.NoError(t, err, "OptIgnoreUnique")
}
