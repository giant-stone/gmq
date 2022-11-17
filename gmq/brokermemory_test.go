package gmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/go/grand"
)

func TestBrokerInMemory_Enqueue(t *testing.T) {
	broker, err := gmq.NewBrokerInMemory(&gmq.BrokerInMemoryOpts{
		MaxBytes: 1024 * 1024,
	})
	require.NoError(t, err)

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err)

	id := fmt.Sprintf("%d", time.Now().UnixMilli())
	type Payload struct {
		Fulluri string
		Data    string
	}
	p := Payload{Fulluri: "https://foo.bar", Data: "hello"}
	dat, _ := json.Marshal(p)
	msgWant := &gmq.Msg{Payload: dat, Id: id}

	msgGot, err := cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err)

	require.Equal(t, msgWant.Id, msgGot.Id)
	require.Equal(t, gmq.DefaultQueueName, msgGot.Queue)
	require.Equal(t, msgWant.Payload, msgGot.GetPayload())
	require.Equal(t, gmq.MsgStatePending, msgGot.State)
	require.Greater(t, msgGot.Created, int64(0))
	require.GreaterOrEqual(t, time.Now().UnixMilli(), msgGot.Created)
	require.Equal(t, msgGot.Expiredat, int64(0))
	require.Equal(t, msgGot.Dieat, int64(0))
	require.Equal(t, msgGot.Err, "")
}

func TestBrokerInMemory_GetMsg(t *testing.T) {
	broker, err := gmq.NewBrokerInMemory(&gmq.BrokerInMemoryOpts{
		MaxBytes: 1024 * 1024,
	})
	require.NoError(t, err)

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err)

	id := fmt.Sprintf("%d", time.Now().UnixMilli())
	type Payload struct {
		Fulluri string
		Data    string
	}
	p := Payload{Fulluri: "https://foo.bar", Data: "hello"}
	dat, _ := json.Marshal(p)

	msgWant := &gmq.Msg{Payload: dat, Id: id, Queue: grand.String(10)}

	rsEnqueue, err := cli.Enqueue(context.Background(), msgWant)
	require.NoError(t, err)
	gotPayload := rsEnqueue.GetPayload()
	require.Equal(t, msgWant.Payload, gotPayload)

	msgGot, err := broker.GetMsg(context.Background(), msgWant.GetQueue(), id)
	require.NoError(t, err)
	require.Equal(t, msgWant.Id, msgGot.Id)
	require.Equal(t, msgWant.Queue, msgGot.Queue)
	require.Equal(t, msgWant.Payload, msgGot.GetPayload())
	require.Equal(t, gmq.MsgStatePending, msgGot.State)
	require.Greater(t, msgGot.Created, int64(0))
	require.GreaterOrEqual(t, time.Now().UnixMilli(), msgGot.Created)
	require.Equal(t, msgGot.Expiredat, int64(0))
	require.Equal(t, msgGot.Dieat, int64(0))
	require.Equal(t, msgGot.Err, "")
}

func TestBrokerInMemory_Dequeue(t *testing.T) {
	broker, err := gmq.NewBrokerInMemory(&gmq.BrokerInMemoryOpts{
		MaxBytes: 1024 * 1024,
	})
	require.NoError(t, err)

	cli, err := gmq.NewClientFromBroker(broker)
	require.NoError(t, err)

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

	require.Equal(t, msgWant.Id, msgGot.Id)
	require.Equal(t, gmq.DefaultQueueName, msgGot.Queue)
	require.Equal(t, msgWant.Payload, msgGot.GetPayload())
	require.Equal(t, gmq.MsgStateProcessing, msgGot.State)
	require.Greater(t, msgGot.Created, int64(0))
	require.GreaterOrEqual(t, time.Now().UnixMilli(), msgGot.Created)
	require.Equal(t, msgGot.Expiredat, int64(0))
	require.Equal(t, msgGot.Dieat, int64(0))
	require.Equal(t, msgGot.Err, "")
}
