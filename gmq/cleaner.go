package gmq

import (
	"context"
	"time"
)

type Cleaner struct {
	ctx        context.Context
	broker     Broker
	logger     Logger
	queueNames map[string]struct{}
}

type CleanerParams struct {
	ctx        context.Context
	broker     Broker
	logger     Logger
	queueNames map[string]struct{}
}

func NewCleaner(params CleanerParams) *Cleaner {
	return &Cleaner{
		ctx:        params.ctx,
		broker:     params.broker,
		logger:     params.logger,
		queueNames: params.queueNames,
	}
}

func (it *Cleaner) start() {
	go func() {
		t := time.NewTicker(TTLDeadMsg)

		for {
			select {
			case <-it.ctx.Done():
				{
					it.logger.Debug("Cleaner done")
					return
				}
			case <-t.C:
				{
					for queueName := range it.queueNames {
						it.broker.DeleteAgo(it.ctx, queueName, TTLMsg)
					}
				}
			}
		}
	}()
}
