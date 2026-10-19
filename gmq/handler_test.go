package gmq_test

import (
	"context"
	"errors"
	"testing"

	"github.com/giant-stone/gmq/gmq"
	"github.com/stretchr/testify/require"
)

// A handler needs only ProcessMsg, without embedding library scaffolding.
type messageHandler struct {
	process func(context.Context, gmq.IMsg) error
}

func (h messageHandler) ProcessMsg(ctx context.Context, msg gmq.IMsg) error {
	return h.process(ctx, msg)
}

func TestHandlerDispatch(t *testing.T) {
	expected := errors.New("processing failed")
	msg := &gmq.Msg{Queue: "reports"}
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "processing context")
	process := func(actual context.Context, actualMsg gmq.IMsg) error {
		require.Equal(t, "processing context", actual.Value(contextKey{}))
		require.Same(t, msg, actualMsg)
		return expected
	}
	handlers := map[string]gmq.IHandler{
		"function": gmq.HandlerFunc(process),
		"struct":   messageHandler{process: process},
	}
	for name, handler := range handlers {
		t.Run(name, func(t *testing.T) {
			mux := gmq.NewMux()
			mux.Handle("reports", handler)
			require.ErrorIs(t, mux.ProcessMsg(ctx, msg), expected)
		})
	}
}

func TestMuxPatternsSnapshot(t *testing.T) {
	mux := gmq.NewMux()
	require.Empty(t, mux.GetPatterns())
	handler := gmq.HandlerFunc(func(context.Context, gmq.IMsg) error { return nil })
	mux.Handle("reports", handler)
	mux.Handle("alerts", handler)
	patterns := mux.GetPatterns()
	require.Equal(t, []string{"alerts", "reports"}, patterns)
	patterns[0] = "changed"
	mux.Handle("updates", handler)
	require.Equal(t, []string{"alerts", "reports", "updates"}, mux.GetPatterns())
	require.Len(t, patterns, 2)
	require.NoError(t, mux.ProcessMsg(context.Background(), &gmq.Msg{Queue: "alerts"}))
}
