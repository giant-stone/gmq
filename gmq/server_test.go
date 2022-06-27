package gmq_test

import (
	"context"
	"testing"

	"github.com/giant-stone/gmq/gmq"
	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	broker := setup(t)
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
