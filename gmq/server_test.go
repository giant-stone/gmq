package gmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/giant-stone/gmq/internal/testutil"
	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

func TestGmq_NewServer(t *testing.T) {
	testCtx := testutil.Context(t)
	broker := setupBrokerRedis(t)
	defer broker.Close()

	srv := gmq.NewServer(testCtx, broker, nil)

	mux := gmq.NewMux()
	mux.Handle("*", gmq.HandlerFunc(func(context.Context, gmq.IMsg) error {
		return nil
	}))

	err := srv.Run(mux)
	require.NoError(t, err, "srv.Run")

	srv.Shutdown()
}

func TestGmq_NewServerErrNoHandlers(t *testing.T) {
	testCtx := testutil.Context(t)
	broker := setupBrokerRedis(t)
	defer broker.Close()

	srv := gmq.NewServer(testCtx, broker, nil)
	mux := gmq.NewMux()
	err := srv.Run(mux)
	require.EqualErrorf(t, err, "no handler(s)", "want err no handlers")
	srv.Shutdown()

	srv = gmq.NewServer(testCtx, broker, nil)
	err = srv.Run(nil)
	require.EqualErrorf(t, err, "no handler(s)", "want err no handlers")
	srv.Shutdown()
}

func TestGmq_NewServerDefaultCfg(t *testing.T) {
	testCtx := testutil.Context(t)
	srv := gmq.NewServer(testCtx, nil, nil)
	cfg := srv.Cfg()
	require.Equal(t, time.Second*time.Duration(gmq.TTLMsg), cfg.MsgMaxTTL)
	require.Equal(t, gmq.DefaultDurationRestIfNoMsg, cfg.RestIfNoMsg)
}
