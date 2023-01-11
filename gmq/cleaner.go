package gmq

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// Cleaner auto-delete dead or failed messages, completed messages at intervals.
type Cleaner struct {
	broker Broker
	ctx    context.Context
	// rate limiter to prevent spamming logs with a bunch of errors.
	errLogLimiter *rate.Limiter
	logger        Logger
	msgMaxTTL     time.Duration
	queueNames    map[string]struct{}
}

type CleanerParams struct {
	Broker     Broker
	Ctx        context.Context
	DeleteAgo  time.Duration
	Logger     Logger
	MsgMaxTTL  time.Duration
	QueueNames map[string]struct{}
}

func NewCleaner(params CleanerParams) *Cleaner {
	return &Cleaner{
		broker:        params.Broker,
		ctx:           params.Ctx,
		errLogLimiter: rate.NewLimiter(rate.Every(2*time.Second), 1),
		logger:        params.Logger,
		msgMaxTTL:     params.MsgMaxTTL,
		queueNames:    params.QueueNames,
	}
}

func (it *Cleaner) start() {
	go func() {
		t := time.NewTicker(it.msgMaxTTL)

		for {
			select {
			case <-it.ctx.Done():
				{
					it.logger.Debug("Cleaner done")
					return
				}
			case <-t.C:
				{
					//TBD 是否要对超过一定时间的统计数据进行清理？
					for queueName := range it.queueNames {
						err := it.broker.DeleteAgo(it.ctx, queueName, it.msgMaxTTL)
						if err != nil && it.errLogLimiter.Allow() {
							it.logger.Errorf("DeleteAgo %v, queue=%s msgMaxTTL=%d", err, queueName, it.msgMaxTTL)
						}
					}
				}
			}
		}
	}()
}
