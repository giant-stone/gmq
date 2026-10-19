package gmq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/stretchr/testify/require"
)

type elapsedLogger struct {
	Logger
	debug    []string
	warnings []string
}

func (l *elapsedLogger) Debugf(template string, args ...interface{}) {
	l.debug = append(l.debug, fmt.Sprintf(template, args...))
}

func (l *elapsedLogger) Warnf(template string, args ...interface{}) {
	l.warnings = append(l.warnings, fmt.Sprintf(template, args...))
}

func TestLoggingElapsedThreshold(t *testing.T) {
	const threshold = time.Second
	failure := errors.New("handler failed")
	for _, test := range []struct {
		name      string
		threshold time.Duration
		elapsed   time.Duration
		err       error
		warn      bool
		slow      bool
	}{
		{"below", threshold, threshold - time.Nanosecond, nil, false, false},
		{"equal", threshold, threshold, nil, true, true},
		{"above", threshold, threshold + time.Nanosecond, nil, true, true},
		{"disabled", 0, 2 * threshold, nil, false, false},
		{"negative", -threshold, 2 * threshold, nil, false, false},
		{"failure", threshold, threshold / 2, failure, true, false},
		{"slow failure", threshold, 2 * threshold, failure, true, true},
		{"disabled failure", 0, 2 * threshold, failure, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := NewSimulatedClock(time.Now())
			logger := &elapsedLogger{}
			ctx := context.WithValue(context.Background(), elapsedContextKey{}, "original")
			msg := &Msg{Id: "message-1", Queue: "reports", Payload: []byte("private payload")}
			calls := 0
			handler := loggingElapsedWithClock(logger, test.threshold, clock)(HandlerFunc(func(gotCtx context.Context, gotMsg IMsg) error {
				calls++
				require.Equal(t, ctx, gotCtx)
				require.Same(t, msg, gotMsg)
				clock.AdvanceTime(test.elapsed)
				return test.err
			}))
			require.True(t, handler.ProcessMsg(ctx, msg) == test.err)
			require.Equal(t, 1, calls)
			require.Equal(t, 1, len(logger.debug)+len(logger.warnings))
			logs := logger.debug
			if test.warn {
				require.Len(t, logger.warnings, 1)
				logs = logger.warnings
			} else {
				require.Len(t, logger.debug, 1)
			}
			require.Contains(t, logs[0], "queue=reports msgId=message-1")
			require.Contains(t, logs[0], "elapsed="+test.elapsed.String())
			require.NotContains(t, logs[0], "private payload")
			if test.warn {
				require.Contains(t, logs[0], "threshold="+test.threshold.String())
			}
			if test.err != nil {
				require.Contains(t, logs[0], fmt.Sprintf("slow=%t error=handler failed", test.slow))
			}
		})
	}
}

type elapsedContextKey struct{}

func TestLoggingElapsedLoggerSelection(t *testing.T) {
	logger := &elapsedLogger{}
	handler := HandlerFunc(func(context.Context, IMsg) error { return nil })
	msg := &Msg{Id: "message-1", Queue: "reports"}
	require.NoError(t, LoggingElapsedWithThreshold(logger, 0)(handler).ProcessMsg(context.Background(), msg))
	require.Len(t, logger.debug, 1)

	glogging.Init([]string{"stderr"}, "warn")
	require.NoError(t, LoggingElapsedWithThreshold(nil, 0)(handler).ProcessMsg(context.Background(), msg))
	require.NoError(t, LoggingElapsed(handler).ProcessMsg(context.Background(), msg))
}
