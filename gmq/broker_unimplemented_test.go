package gmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/stretchr/testify/require"
)

type enqueueOnlyBroker struct {
	gmq.BrokerUnimplemented
}

func (enqueueOnlyBroker) Enqueue(_ context.Context, msg gmq.IMsg, _ ...gmq.OptionClient) (*gmq.Msg, error) {
	return &gmq.Msg{Id: msg.GetId(), Queue: msg.GetQueue()}, nil
}

var _ gmq.Broker = enqueueOnlyBroker{}

func TestBrokerUnimplementedEmbedding(t *testing.T) {
	client, err := gmq.NewClientFromBroker(enqueueOnlyBroker{})
	require.NoError(t, err)
	msg, err := client.Enqueue(context.Background(), &gmq.Msg{Id: "report", Queue: "reports"})
	require.NoError(t, err)
	require.Equal(t, "report", msg.Id)
	require.Equal(t, "reports", msg.Queue)
	require.NoError(t, client.Close())
}

func TestBrokerUnimplementedDefaults(t *testing.T) {
	var broker gmq.Broker = gmq.BrokerUnimplemented{}
	ctx := context.Background()
	msg := &gmq.Msg{Queue: "reports", Id: "report"}
	require.NoError(t, broker.Close())
	require.NotPanics(t, func() {
		broker.SetClock(gmq.NewWallClock())
		broker.UTC(true)
	})
	operations := map[string]func() error{
		"Complete":    func() error { return broker.Complete(ctx, msg) },
		"Dequeue":     func() error { value, err := broker.Dequeue(ctx, msg.Queue); require.Nil(t, value); return err },
		"DeleteMsg":   func() error { return broker.DeleteMsg(ctx, msg.Queue, msg.Id) },
		"DeleteQueue": func() error { return broker.DeleteQueue(ctx, msg.Queue) },
		"DeleteAgo":   func() error { return broker.DeleteAgo(ctx, msg.Queue, time.Hour) },
		"Enqueue":     func() error { value, err := broker.Enqueue(ctx, msg); require.Nil(t, value); return err },
		"Fail":        func() error { return broker.Fail(ctx, msg, gmq.ErrInternal) },
		"GetMsg":      func() error { value, err := broker.GetMsg(ctx, msg.Queue, msg.Id); require.Nil(t, value); return err },
		"ListMsg": func() error {
			value, err := broker.ListMsg(ctx, msg.Queue, gmq.MsgStatePending, 1, 0)
			require.Nil(t, value)
			return err
		},
		"ListFailed": func() error {
			value, err := broker.ListFailed(ctx, msg.Queue, msg.Id, 1, 0)
			require.Nil(t, value)
			return err
		},
		"ListQueue": func() error { value, err := broker.ListQueue(ctx); require.Nil(t, value); return err },
		"GetStats":  func() error { value, err := broker.GetStats(ctx); require.Nil(t, value); return err },
		"Init":      func() error { return broker.Init(ctx, msg.Queue) },
		"Ping":      func() error { return broker.Ping(ctx) },
		"GetStatsByDate": func() error {
			value, err := broker.GetStatsByDate(ctx, []string{msg.Queue}, "2026-10-19")
			require.Nil(t, value)
			return err
		},
		"GetStatsWeekly": func() error {
			value, err := broker.GetStatsWeekly(ctx, []string{msg.Queue})
			require.Nil(t, value)
			return err
		},
		"Pause":  func() error { return broker.Pause(ctx, msg.Queue) },
		"Resume": func() error { return broker.Resume(ctx, msg.Queue) },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			err := operation()
			require.ErrorIs(t, err, gmq.ErrNotImplemented)
			require.Contains(t, err.Error(), name)
		})
	}
}
