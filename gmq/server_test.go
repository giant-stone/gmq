package gmq_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/giant-stone/gmq/gmq"
)

func TestGmq_NewServer(t *testing.T) {
	broker := setupBrokerRedis(t)
	defer broker.Close()

	srv := gmq.NewServer(context.Background(), broker, nil)

	mux := gmq.NewMux()
	mux.Handle("*", gmq.HandlerFunc(func(context.Context, gmq.IMsg) error {
		return nil
	}))

	err := srv.Run(mux)
	require.NoError(t, err, "srv.Run")

	srv.Shutdown()
}

func TestGmq_NewServerErrNoHandlers(t *testing.T) {
	broker := setupBrokerRedis(t)
	defer broker.Close()

	srv := gmq.NewServer(context.Background(), broker, nil)
	mux := gmq.NewMux()
	err := srv.Run(mux)
	require.EqualErrorf(t, err, "no handler(s)", "want err no handlers")
	srv.Shutdown()

	srv = gmq.NewServer(context.Background(), broker, nil)
	err = srv.Run(nil)
	require.EqualErrorf(t, err, "no handler(s)", "want err no handlers")
	srv.Shutdown()
}
