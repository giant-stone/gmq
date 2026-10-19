package gmq_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/gmq/internal/test_util"
	"github.com/stretchr/testify/require"
)

func openSQLiteBroker(t *testing.T, filename string) gmq.Broker {
	t.Helper()
	broker, err := gmq.NewBrokerSQLite(filename)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, broker.Close()) })
	return broker
}

func TestBrokerSQLite_Compatibility(t *testing.T) {
	checks := []struct {
		name  string
		check func(*testing.T, gmq.Broker)
	}{
		{"Enqueue", gmq.CheckBrokerEnqueue},
		{"GetMsg", gmq.CheckBrokerGetMsg},
		{"Dequeue", gmq.CheckBrokerDequeue},
		{"DeleteMsg", gmq.CheckBrokerDeleteMsg},
		{"DeleteQueue", gmq.CheckBrokerDeleteQueue},
		{"DeleteAgo", gmq.CheckBrokerDeleteAgo},
		{"Complete", gmq.CheckBrokerComplete},
		{"Fail", gmq.CheckBrokerFail},
		{"ListQueue", gmq.CheckBrokerListQueue},
		{"ListMsg", gmq.CheckBrokerListMsg},
		{"ListFailed", gmq.CheckBrokerListFailed},
		{"ListFailedMaxItems", gmq.CheckBrokerListFailedMaxItems},
		{"GetStats", gmq.CheckBrokerGetStats},
		{"GetStatsByDate", gmq.CheckBrokerGetStatsByDate},
		{"Deduplicate", gmq.CheckBrokerAutoDeduplicateMsgByDefault},
		{"DeduplicateFailed", gmq.CheckBrokerAutoDeduplicateFailedMsg},
		{"DeduplicateCompleted", gmq.CheckBrokerAutoDeduplicateCompletedMsg},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			check.check(t, openSQLiteBroker(t, ":memory:"))
		})
	}
}

func TestBrokerSQLite_Persistence(t *testing.T) {
	ctx := test_util.Context(t)
	filename := filepath.Join(t.TempDir(), "queue.db")
	broker := openSQLiteBroker(t, filename)
	for _, id := range []string{"pending", "processing", "completed", "failed"} {
		_, err := broker.Enqueue(ctx, &gmq.Msg{Queue: id, Id: id, Payload: []byte(id)})
		require.NoError(t, err)
		if id == "pending" {
			continue
		}
		msg, err := broker.Dequeue(ctx, id)
		require.NoError(t, err)
		switch id {
		case "completed":
			require.NoError(t, broker.Complete(ctx, msg))
		case "failed":
			require.NoError(t, broker.Fail(ctx, msg, errors.New("handler failed")))
		}
	}
	require.NoError(t, broker.Pause(ctx, "pending"))
	require.NoError(t, broker.Close())
	broker = openSQLiteBroker(t, filename)
	_, err := broker.Dequeue(ctx, "pending")
	require.ErrorIs(t, err, gmq.ErrNoMsg)
	require.NoError(t, broker.Resume(ctx, "pending"))
	msg, err := broker.Dequeue(ctx, "pending")
	require.NoError(t, err)
	require.Equal(t, []byte("pending"), msg.Payload)
	msg, err = broker.GetMsg(ctx, "processing", "processing")
	require.NoError(t, err)
	require.Equal(t, gmq.MsgStateProcessing, msg.State)
	for _, id := range []string{"completed", "failed"} {
		_, err = broker.Enqueue(ctx, &gmq.Msg{Queue: id, Id: id})
		require.ErrorIs(t, err, gmq.ErrMsgIdConflict)
		_, err = broker.GetMsg(ctx, id, id)
		require.ErrorIs(t, err, gmq.ErrNoMsg)
	}
	history, err := broker.ListFailed(ctx, "failed", "failed", 0, 0)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, "handler failed", history[0].Err)
	stats, err := broker.GetStatsByDate(ctx, []string{"failed", "completed"}, time.Now().Format("2006-01-02"))
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.Total)
}

func TestBrokerSQLite_ConcurrentClaims(t *testing.T) {
	const messageCount = 40
	ctx := test_util.Context(t)
	filename := filepath.Join(t.TempDir(), "queue.db")
	first := openSQLiteBroker(t, filename)
	second := openSQLiteBroker(t, filename)
	for i := 0; i < messageCount; i++ {
		_, err := first.Enqueue(ctx, &gmq.Msg{Id: fmt.Sprint(i)})
		require.NoError(t, err)
	}
	ids := make(chan string, messageCount)
	errorsFound := make(chan error, messageCount)
	var workers sync.WaitGroup
	for _, broker := range []gmq.Broker{first, second} {
		workers.Add(1)
		go func(broker gmq.Broker) {
			defer workers.Done()
			for {
				msg, err := broker.Dequeue(ctx, gmq.DefaultQueueName)
				if errors.Is(err, gmq.ErrNoMsg) {
					return
				}
				if err != nil {
					errorsFound <- err
					return
				}
				ids <- msg.Id
				if err = broker.Complete(ctx, msg); err != nil {
					errorsFound <- err
					return
				}
			}
		}(broker)
	}
	workers.Wait()
	close(ids)
	close(errorsFound)
	for err := range errorsFound {
		require.NoError(t, err)
	}
	seen := map[string]bool{}
	for id := range ids {
		require.False(t, seen[id], "message %s claimed twice", id)
		seen[id] = true
	}
	require.Len(t, seen, messageCount)
}

func TestBrokerSQLite_PaginationAndCleanup(t *testing.T) {
	ctx := test_util.Context(t)
	broker := openSQLiteBroker(t, ":memory:")
	clock := gmq.NewSimulatedClock(time.Now())
	broker.SetClock(clock)
	for _, id := range []string{"first", "second", "third"} {
		_, err := broker.Enqueue(ctx, &gmq.Msg{Id: id})
		require.NoError(t, err)
	}
	ids, err := broker.ListMsg(ctx, gmq.DefaultQueueName, gmq.MsgStatePending, 1, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"second"}, ids)
	for _, id := range []string{"first", "second", "third"} {
		msg, err := broker.Dequeue(ctx, gmq.DefaultQueueName)
		require.NoError(t, err)
		require.Equal(t, id, msg.Id)
	}
	clock.AdvanceTime(time.Hour)
	require.NoError(t, broker.DeleteAgo(ctx, gmq.DefaultQueueName, time.Minute))
	stats, err := broker.GetStats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats[0].Total)
	require.NoError(t, broker.DeleteQueue(ctx, gmq.DefaultQueueName))
	queues, err := broker.ListQueue(ctx)
	require.NoError(t, err)
	require.Empty(t, queues)
	_, err = broker.Enqueue(ctx, &gmq.Msg{Id: "first"})
	require.NoError(t, err)
}

func TestBrokerSQLite_ContextAndOpenErrors(t *testing.T) {
	broker, err := gmq.NewBrokerSQLite("")
	require.Error(t, err)
	require.Nil(t, broker)
	broker, err = gmq.NewBrokerSQLite(filepath.Join(t.TempDir(), "missing", "queue.db"))
	require.Error(t, err)
	require.Nil(t, broker)
	broker = openSQLiteBroker(t, ":memory:")
	ctx, cancel := context.WithCancel(test_util.Context(t))
	cancel()
	_, err = broker.Enqueue(ctx, &gmq.Msg{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = broker.Dequeue(ctx, gmq.DefaultQueueName)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, broker.Ping(ctx), context.Canceled)
}

func TestBrokerSQLite_OptionsAndDates(t *testing.T) {
	ctx := test_util.Context(t)
	broker := openSQLiteBroker(t, ":memory:")
	clock := gmq.NewSimulatedClock(time.Date(2026, time.October, 19, 12, 0, 0, 0, time.UTC))
	broker.SetClock(clock)
	broker.UTC(true)
	msg, err := broker.Enqueue(ctx, &gmq.Msg{Queue: "original"}, gmq.OptQueueName("reports"), gmq.OptUniqueIn(time.Hour))
	require.NoError(t, err)
	require.NotEmpty(t, msg.Id)
	require.Equal(t, "reports", msg.Queue)
	require.Equal(t, clock.Now().Add(time.Hour).UnixMilli(), msg.ExpiresAt)
	require.NoError(t, broker.Complete(ctx, msg))
	clock.AdvanceTime(time.Hour)
	_, err = broker.Enqueue(ctx, msg, gmq.OptUniqueIn(time.Hour))
	require.NoError(t, err, "uniqueness expires at the exact deadline")
	require.NoError(t, broker.Fail(ctx, msg, errors.New("failed report")))
	clock.AdvanceTime(24 * time.Hour)
	_, err = broker.Enqueue(ctx, &gmq.Msg{Queue: "other"})
	require.NoError(t, err)
	other, err := broker.Dequeue(ctx, "other")
	require.NoError(t, err)
	require.NoError(t, broker.Complete(ctx, other))
	stat, err := broker.GetStatsByDate(ctx, []string{"reports", "reports"}, "2026-10-19")
	require.NoError(t, err)
	require.EqualValues(t, 1, stat.Completed)
	require.EqualValues(t, 1, stat.Failed)
	weekly, err := broker.GetStatsWeekly(ctx, []string{"reports"})
	require.NoError(t, err)
	require.Len(t, weekly, 8)
	require.Equal(t, "2026-10-13", weekly[0].Date)
	require.EqualValues(t, 2, weekly[6].Total)
	require.Zero(t, weekly[7].Total)
	require.NoError(t, broker.DeleteAgo(ctx, "reports", time.Hour))
	history, err := broker.ListFailed(ctx, "reports", msg.Id, 0, 0)
	require.NoError(t, err)
	require.Empty(t, history)
	stat, err = broker.GetStatsByDate(ctx, []string{"reports"}, "2026-10-19")
	require.NoError(t, err)
	require.EqualValues(t, 2, stat.Total, "cleanup retains daily statistics")
	require.NoError(t, broker.DeleteQueue(ctx, "reports"))
	stat, err = broker.GetStatsByDate(ctx, []string{"reports"}, "2026-10-19")
	require.NoError(t, err)
	require.Zero(t, stat.Total)
}

func TestBrokerSQLite_ConcurrentDeduplication(t *testing.T) {
	ctx := test_util.Context(t)
	filename := filepath.Join(t.TempDir(), "queue.db")
	brokers := []gmq.Broker{openSQLiteBroker(t, filename), openSQLiteBroker(t, filename)}
	results := make(chan error, len(brokers))
	for _, broker := range brokers {
		go func(broker gmq.Broker) {
			_, err := broker.Enqueue(ctx, &gmq.Msg{Id: "shared"})
			results <- err
		}(broker)
	}
	succeeded, conflicted := 0, 0
	for range brokers {
		err := <-results
		if err == nil {
			succeeded++
		} else {
			require.ErrorIs(t, err, gmq.ErrMsgIdConflict)
			conflicted++
		}
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, conflicted)
}

func TestBrokerSQLite_SchemaVersion(t *testing.T) {
	ctx := test_util.Context(t)
	filename := filepath.Join(t.TempDir(), "queue.db")
	db, err := sql.Open("sqlite", filename)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "PRAGMA user_version = 2")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	broker, err := gmq.NewBrokerSQLite(filename)
	require.ErrorContains(t, err, "exceeds supported version")
	require.Nil(t, broker)
}
