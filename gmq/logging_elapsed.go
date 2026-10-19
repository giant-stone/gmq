package gmq

import (
	"context"
	"time"

	"github.com/giant-stone/go/glogging"
)

// LoggingElapsed logs handler results without a slow threshold using the default logger.
func LoggingElapsed(h IHandler) IHandler {
	return LoggingElapsedWithThreshold(nil, 0)(h)
}

// LoggingElapsedWithThreshold logs successful handlers at Warn when their elapsed
// time is at least threshold, otherwise at Debug. Failures always produce one Warn.
// A threshold of zero or less disables slow detection; a nil logger uses the default
// logger. Timing excludes queue waiting, broker operations, and worker intervals.
func LoggingElapsedWithThreshold(logger Logger, threshold time.Duration) MiddlewareFunc {
	if logger == nil {
		logger = glogging.Sugared
	}
	return loggingElapsedWithClock(logger, threshold, NewWallClock())
}

func loggingElapsedWithClock(logger Logger, threshold time.Duration, clock Clock) MiddlewareFunc {
	return func(h IHandler) IHandler {
		return HandlerFunc(func(ctx context.Context, msg IMsg) error {
			start := clock.Now()
			err := h.ProcessMsg(ctx, msg)
			elapsed := clock.Now().Sub(start)
			slow := threshold > 0 && elapsed >= threshold
			switch {
			case err != nil:
				logger.Warnf("ProcessMsg failed queue=%s msgId=%s elapsed=%s threshold=%s slow=%t error=%v",
					msg.GetQueue(), msg.GetId(), elapsed, threshold, slow, err)
			case slow:
				logger.Warnf("ProcessMsg slow queue=%s msgId=%s elapsed=%s threshold=%s",
					msg.GetQueue(), msg.GetId(), elapsed, threshold)
			default:
				logger.Debugf("ProcessMsg success queue=%s msgId=%s elapsed=%s", msg.GetQueue(), msg.GetId(), elapsed)
			}
			return err
		})
	}
}
