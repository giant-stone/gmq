package gmq

import (
	"context"
	"time"
)

// Cleaner auto-delete dead or failed messages, completed messages at intervals.
type Cleaner struct {
	ctx        context.Context
	broker     Broker
	logger     Logger
	queueNames map[string]struct{}
}

type CleanerParams struct {
	Ctx        context.Context
	Broker     Broker
	Logger     Logger
	QueueNames map[string]struct{}
}

func NewCleaner(params CleanerParams) *Cleaner {
	return &Cleaner{
		ctx:        params.Ctx,
		broker:     params.Broker,
		logger:     params.Logger,
		queueNames: params.QueueNames,
	}
}

func (it *Cleaner) start() {
	go func() {
		t := time.NewTicker(TTLDeadMsg * time.Second)

		for {
			select {
			case <-it.ctx.Done():
				{
					it.logger.Debug("Cleaner done")
					return
				}
			case <-t.C:
				{ //TBD 是否要对超过一定时间的统计数据进行清理？
					for queueName := range it.queueNames {
						err := it.broker.DeleteAgo(it.ctx, queueName, TTLMsg)
						it.logger.Errorf("queue: %s os:server.Clean error(%)", queueName, err)
					}
				}
			}
		}
	}()
}
