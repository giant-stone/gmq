package gmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

func TestGmq_NewScheduler(t *testing.T) {
	broker := setupBrokerRedis(t)
	defer broker.Close()

	scheduler := gmq.NewScheduler(gmq.SchedulerParams{Ctx: context.Background(), Broker: broker})

	err := scheduler.Run()
	require.NoError(t, err, "scheduler.Run")

	scheduler.Shutdown()
}

func TestScheduler_Register(t *testing.T) {
	samples := []struct {
		cronSpec string
		wait     time.Duration
		wantMsg  gmq.IMsg
		wantErr  error
	}{
		{
			cronSpec: "@every 1s",
			wait:     time.Second * time.Duration(1),
			wantMsg:  &gmq.Msg{Payload: []byte(`{"msg":"hello"}`), Queue: "mycron", Id: fmt.Sprintf("msg%d", time.Now().UnixMilli())},
			wantErr:  nil,
		},
	}

	for _, sample := range samples {
		broker := setupBrokerRedis(t)
		defer broker.Close()

		now := time.Now()

		ctx := context.Background()
		scheduler := gmq.NewScheduler(gmq.SchedulerParams{Ctx: ctx, Broker: broker})

		jobId, err := scheduler.Register(sample.cronSpec, sample.wantMsg)
		require.NoError(t, err, "scheduler.Register")
		require.NotEmpty(t, jobId, "jobId")

		err = scheduler.Run()
		require.NoError(t, err, "scheduler.Run")

		_, err = broker.GetMsg(ctx, sample.wantMsg.GetQueue(), sample.wantMsg.GetId())
		require.ErrorIs(t, err, gmq.ErrNoMsg, "broker.GetMsg")

		time.Sleep(sample.wait)
		scheduler.Shutdown()

		gotMsg, err := broker.GetMsg(ctx, sample.wantMsg.GetQueue(), sample.wantMsg.GetId())
		require.NoError(t, err, "broker.GetMsg")
		require.Equal(t, sample.wantMsg.GetQueue(), gotMsg.GetQueue(), "GetQueue")
		require.Equal(t, sample.wantMsg.GetId(), gotMsg.GetId(), "GetId")
		require.Equal(t, sample.wantMsg.GetPayload(), gotMsg.GetPayload(), "GetPayload")
		require.Equal(t, sample.wantMsg.GetPayload(), gotMsg.GetPayload(), "GetPayload")
		require.Equal(t, gmq.MsgStatePending, gotMsg.State, "State")
		require.Equal(t, now.Add(sample.wait).Unix(), gotMsg.Created/1000, "Created")
		require.Equal(t, now.Add(sample.wait).Unix(), gotMsg.Updated/1000, "Updated")

		gotMsg, err = broker.Dequeue(ctx, sample.wantMsg.GetQueue())
		require.NoError(t, err, "broker.Dequeue")
		require.Equal(t, sample.wantMsg.GetQueue(), gotMsg.GetQueue(), "GetQueue")
		require.Equal(t, sample.wantMsg.GetId(), gotMsg.GetId(), "GetId")
		require.Equal(t, sample.wantMsg.GetPayload(), gotMsg.GetPayload(), "GetPayload")
		require.Equal(t, sample.wantMsg.GetPayload(), gotMsg.GetPayload(), "GetPayload")
		require.Equal(t, gmq.MsgStateProcessing, gotMsg.State, "State")
		require.Equal(t, now.Add(sample.wait).Unix(), gotMsg.Created/1000, "Created")
		require.LessOrEqual(t, now.UnixMilli(), gotMsg.Updated, "Updated")

		_, err = broker.Dequeue(ctx, sample.wantMsg.GetQueue())
		require.Error(t, err, gmq.ErrNoMsg)
	}
}

func TestScheduler_Unregister(t *testing.T) {
	samples := []struct {
		cronSpec string
		wait     time.Duration
		wantMsg  gmq.IMsg
		wantErr  error
	}{
		{
			cronSpec: "@every 1s",
			wait:     time.Second * time.Duration(1),
			wantMsg:  &gmq.Msg{Payload: []byte(`{"msg":"hello"}`), Queue: "mycron"},
			wantErr:  nil,
		},
	}

	for _, sample := range samples {
		broker := setupBrokerRedis(t)
		defer broker.Close()

		ctx := context.Background()
		scheduler := gmq.NewScheduler(gmq.SchedulerParams{Ctx: ctx, Broker: broker})

		jobId, err := scheduler.Register(sample.cronSpec, sample.wantMsg)
		require.NoError(t, err, "scheduler.Register")
		require.NotEmpty(t, jobId, "jobId")

		err = scheduler.Run()
		require.NoError(t, err, "scheduler.Run")

		queueStatList, err := broker.GetStats(ctx)
		require.NoError(t, err, "broker.GetStats")
		require.Equal(t, 0, len(queueStatList))

		time.Sleep(sample.wait)

		queueStatList, err = broker.GetStats(ctx)
		require.NoError(t, err, "broker.GetStats")
		require.Equal(t, 1, len(queueStatList))
		queueState := queueStatList[0]
		require.Equal(t, sample.wantMsg.GetQueue(), queueState.Name)
		require.Equal(t, int64(1), queueState.Total)
		require.Equal(t, int64(1), queueState.Pending)
		require.Equal(t, int64(0), queueState.Processing)

		_, err = broker.Dequeue(ctx, sample.wantMsg.GetQueue())
		require.NoError(t, err, "broker.Dequeue")

		err = scheduler.Unregister(jobId)
		require.NoError(t, err, "scheduler.Unregister")

		time.Sleep(sample.wait)

		queueStatList, err = broker.GetStats(ctx)
		require.NoError(t, err, "broker.GetStats")
		require.Equal(t, 1, len(queueStatList))
		queueState = queueStatList[0]
		require.Equal(t, sample.wantMsg.GetQueue(), queueState.Name)
		require.Equal(t, int64(1), queueState.Total)
		require.Equal(t, int64(0), queueState.Pending)
		require.Equal(t, int64(1), queueState.Processing)

		_, err = broker.Dequeue(ctx, sample.wantMsg.GetQueue())
		require.ErrorIs(t, err, gmq.ErrNoMsg, "broker.Dequeue")

		time.Sleep(sample.wait)

		_, err = broker.Dequeue(ctx, sample.wantMsg.GetQueue())
		require.ErrorIs(t, err, gmq.ErrNoMsg, "broker.Dequeue")
	}
}
