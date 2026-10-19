package gmq_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/giant-stone/gmq/gmq"
	"github.com/giant-stone/gmq/internal/test_util"
	"github.com/stretchr/testify/require"
)

type shutdownBroker struct {
	gmq.Broker
	closed        chan struct{}
	cleaning      chan struct{}
	cleanCanceled chan struct{}
	release       chan struct{}
	cleanCount    atomic.Int32
}

func (it *shutdownBroker) Close() error {
	close(it.closed)
	return it.Broker.Close()
}

func (it *shutdownBroker) DeleteAgo(ctx context.Context, queue string, age time.Duration) error {
	if it.cleaning != nil && it.cleanCount.Add(1) == 2 {
		close(it.cleaning)
		<-ctx.Done()
		close(it.cleanCanceled)
		<-it.release
	}
	return it.Broker.DeleteAgo(ctx, queue, age)
}

func TestServerShutdownWaitsForHandler(t *testing.T) {
	ctx := test_util.Context(t)
	broker := &shutdownBroker{Broker: openSQLiteBroker(t, ":memory:"), closed: make(chan struct{})}
	_, err := broker.Enqueue(ctx, &gmq.Msg{})
	require.NoError(t, err)
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	mux := gmq.NewMux()
	mux.Handle(gmq.DefaultQueueName, gmq.HandlerFunc(func(ctx context.Context, msg gmq.IMsg) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil
	}))
	server := gmq.NewServer(ctx, broker, &gmq.Config{
		QueueConfigs: map[string]*gmq.QueueCfg{
			gmq.DefaultQueueName: gmq.NewQueueCfg(gmq.OptQueueWorkerNum(1)),
		},
	})
	require.NoError(t, server.Run(mux))
	waitForShutdownSignal(t, ctx, started)
	stopped := make(chan struct{})
	go func() { server.Shutdown(); close(stopped) }()
	waitForShutdownSignal(t, ctx, canceled)
	select {
	case <-broker.closed:
		t.Error("broker closed before the handler returned")
	default:
	}
	close(release)
	waitForShutdownSignal(t, ctx, stopped)
	waitForShutdownSignal(t, ctx, broker.closed)
}

func TestServerShutdownWaitsForCleaner(t *testing.T) {
	const cleanupInterval = 10 * time.Millisecond
	ctx := test_util.Context(t)
	broker := &shutdownBroker{
		Broker: openSQLiteBroker(t, ":memory:"), closed: make(chan struct{}),
		cleaning: make(chan struct{}), release: make(chan struct{}),
		cleanCanceled: make(chan struct{}),
	}
	mux := gmq.NewMux()
	mux.Handle(gmq.DefaultQueueName, gmq.HandlerFunc(func(context.Context, gmq.IMsg) error { return nil }))
	server := gmq.NewServer(ctx, broker, &gmq.Config{MsgMaxTTL: cleanupInterval * 2})
	require.NoError(t, server.Run(mux))
	waitForShutdownSignal(t, ctx, broker.cleaning)
	stopped := make(chan struct{})
	go func() { server.Shutdown(); close(stopped) }()
	waitForShutdownSignal(t, ctx, broker.cleanCanceled)
	select {
	case <-broker.closed:
		t.Error("broker closed while cleanup was running")
	default:
	}
	close(broker.release)
	waitForShutdownSignal(t, ctx, stopped)
	waitForShutdownSignal(t, ctx, broker.closed)
}

func waitForShutdownSignal(t *testing.T, ctx context.Context, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatal("timed out waiting for shutdown signal")
	}
}
