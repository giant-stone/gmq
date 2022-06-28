package gmq

import (
	"context"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/giant-stone/go/gstr"
)

func LoggingElapsed(h Handler) Handler {
	return HandlerFunc(func(ctx context.Context, msg IMsg) error {
		start := time.Now()
		err := h.ProcessMsg(ctx, msg)
		if err != nil {
			return err
		}

		shorten := gstr.ShortenWith(msg.String(), 100, gstr.DefaultShortenSuffix)
		glogging.Sugared.Debugf("process %s elapsed time %v", shorten, time.Since(start))
		return nil
	})
}
