package gmq_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/gmq/internal/test_util"
)

func TestGmq_NewScheduler(t *testing.T) {
	testCtx := test_util.Context(t)
	broker := setupBrokerRedis(t)
	defer broker.Close()

	scheduler := gmq.NewScheduler(gmq.SchedulerParams{Ctx: testCtx, Broker: broker})

	err := scheduler.Run()
	require.NoError(t, err, "scheduler.Run")

	scheduler.Shutdown()
}

func TestScheduler_Register(t *testing.T) {
	testCtx := test_util.Context(t)
	samples := []struct {
		cronSpec string
		wait     time.Duration
		wantMsg  gmq.IMsg
		wantErr  error
	}{
		{
			cronSpec: "@every 1s",
			wait:     time.Second * time.Duration(1),
			wantMsg:  &gmq.Msg{Payload: []byte(`{"msg":"hello"}`), Queue: "scheduledTask", Id: fmt.Sprintf("msg%d", time.Now().UnixMilli())},
			wantErr:  nil,
		},
	}

	for _, sample := range samples {
		broker := setupBrokerRedis(t)
		defer broker.Close()

		now := time.Now()

		ctx := testCtx
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
	const (
		pollInterval = 10 * time.Millisecond
		waitTimeout  = 5 * time.Second
		cronInterval = time.Second
	)
	ctx := test_util.Context(t)
	broker := setupBrokerRedis(t)
	defer broker.Close()
	scheduler := gmq.NewScheduler(gmq.SchedulerParams{Ctx: ctx, Broker: broker})
	defer scheduler.Shutdown()

	msg := &gmq.Msg{Payload: []byte(`{"msg":"hello"}`), Queue: "scheduledTask"}
	jobId, err := scheduler.Register("@every 1s", msg)
	require.NoError(t, err)
	require.NoError(t, scheduler.Run())
	require.Eventually(t, func() bool {
		stats, err := broker.GetStats(ctx)
		return err == nil && len(stats) == 1 && stats[0].Pending > 0
	}, waitTimeout, pollInterval)
	require.NoError(t, scheduler.Unregister(jobId))
	_, err = broker.Dequeue(ctx, msg.Queue)
	require.NoError(t, err)
	time.Sleep(2 * cronInterval)
	_, err = broker.Dequeue(ctx, msg.Queue)
	require.ErrorIs(t, err, gmq.ErrNoMsg)
	stats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.EqualValues(t, 1, stats[0].Total)
	require.EqualValues(t, 0, stats[0].Pending)
	require.EqualValues(t, 1, stats[0].Processing)
}
