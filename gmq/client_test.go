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

	now := time.Now().UnixNano()
	msgId := fmt.Sprintf("%d", now)
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
	require.Less(t, int64(0), msgGot.Created, "msg.Created")
	require.Equal(t, int64(0), msgGot.Processed, "msg.Processed")

	// validate message via broker lower API
	msgGot, err = broker.Get(context.Background(), queueName, msgId)
	require.NoError(t, err, "broker.Get")
	require.Equal(t, msgWant.GetQueue(), msgGot.GetQueue(), "GetQueue")
	require.Equal(t, msgWant.GetId(), msgGot.GetId(), "GetId")
	require.Equal(t, msgWant.GetPayload(), msgGot.GetPayload(), "GetPayload")
	require.Equal(t, gmq.MsgStatePending, msgGot.State, "msg.State")
	require.Less(t, int64(0), msgGot.Created, "msg.Created")
	require.Equal(t, int64(0), msgGot.Processed, "msg.Processed")
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
