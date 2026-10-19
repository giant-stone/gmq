package gmq

import (
	"context"
	"sync"
	"time"

	"github.com/giant-stone/go/ghuman"
	"golang.org/x/time/rate"
)

// Cleaner auto-delete dead or failed messages, completed messages at intervals.
type Cleaner struct {
	broker  Broker
	ctx     context.Context
	cancel  context.CancelFunc
	workers sync.WaitGroup
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
	ctx, cancel := context.WithCancel(params.Ctx)
	return &Cleaner{
		broker:        params.Broker,
		ctx:           ctx,
		cancel:        cancel,
		errLogLimiter: rate.NewLimiter(rate.Every(1*time.Second), 1),
		logger:        params.Logger,
		msgMaxTTL:     params.MsgMaxTTL,
		queueNames:    params.QueueNames,
	}
}

func (it *Cleaner) start() {
	it.exec()

	it.workers.Add(1)
	go func() {
		defer it.workers.Done()
		t := time.NewTicker(it.msgMaxTTL / 2)
		defer t.Stop()

		for {
			select {
			case <-it.ctx.Done():
				{
					it.logger.Debug("Cleaner done")
					return
				}
			case <-t.C:
				{
					it.exec()
				}
			}
		}
	}()
}

func (it *Cleaner) shutdown() {
	it.cancel()
	it.workers.Wait()
}

func (it *Cleaner) exec() {
	for queueName := range it.queueNames {
		t := time.Now()
		err := it.broker.DeleteAgo(it.ctx, queueName, it.msgMaxTTL)
		elapsed := time.Since(t)

		if it.errLogLimiter.Allow() {
			if err != nil && err != context.Canceled {
				it.logger.Errorf("DeleteAgo %v, queue=%s msgMaxTTL=%s elapsed=%s", err, queueName, ghuman.FmtDuration(it.msgMaxTTL), ghuman.FmtDuration(elapsed))
			} else {
				it.logger.Debugf("DeleteAgo success queue=%s msgMaxTTL=%s elapsed=%s", queueName, ghuman.FmtDuration(it.msgMaxTTL), ghuman.FmtDuration(elapsed))
			}
		}
	}
}
