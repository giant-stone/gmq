package gmq

import (
	"context"
	"errors"
	"runtime"
	"time"

	"golang.org/x/time/rate"
)

type Processor struct {
	ctx    context.Context
	broker Broker
	// rate limiter to prevent spamming logs with a bunch of errors.
	errLogLimiter *rate.Limiter
	handler       Handler
	logger        Logger

	queueName string

	// sema is a counting semaphore to ensure the number of active workers does not exceed the limit.
	sema                   chan struct{}
	workerNum              uint16
	workerWorkIntervalFunc FuncWorkInterval
}

type ProcessorParams struct {
	ctx       context.Context
	broker    Broker
	handler   Handler
	logger    Logger
	queueName string

	workerNum              uint16
	workerWorkIntervalFunc FuncWorkInterval
}

func NewProcessor(params ProcessorParams) *Processor {
	if params.workerNum == 0 {
		params.workerNum = uint16(runtime.NumCPU())
	}

	if params.queueName == "" {
		params.queueName = DefaultQueueName
	}

	return &Processor{
		broker:        params.broker,
		ctx:           params.ctx,
		errLogLimiter: rate.NewLimiter(rate.Every(3*time.Second), 1),
		handler:       params.handler,
		logger:        params.logger,
		sema:          make(chan struct{}, params.workerNum),

		workerNum:              params.workerNum,
		queueName:              params.queueName,
		workerWorkIntervalFunc: params.workerWorkIntervalFunc,
	}
}

func (it *Processor) start() {
	err := it.broker.Init(it.ctx, it.queueName)
	if err != nil {
		it.logger.Error("broker.Init ", err)
	}

	go func() {
		for {
			select {
			case <-it.ctx.Done():
				{
					it.logger.Debug("Processor done")
				}
			default:
				it.exec()
			}
		}
	}()
}

func (it *Processor) exec() {
	select {
	// TBD.
	case it.sema <- struct{}{}:
		{
			msg, err := it.broker.Dequeue(it.ctx, it.queueName)

			switch {
			case errors.Is(err, ErrNoMsg):
				{
					it.logger.Debugf("queue=%s are empty", it.queueName)
					time.Sleep(time.Second)
					<-it.sema
					return
				}
			case err != nil:
				{
					if it.errLogLimiter.Allow() {
						it.logger.Errorf("queue=%s broker.Dequeue %v", it.queueName, err)
					}
					<-it.sema
					return
				}
			}

			if msg == nil {
				it.logger.Warn("msg is empty")
				<-it.sema
				return
			}

			var remain int64
			s := time.Now()
			err = it.handler.ProcessMsg(it.ctx, msg)
			if it.workerWorkIntervalFunc != nil {
				remain = it.workerWorkIntervalFunc().Milliseconds() - time.Since(s).Milliseconds()
			}

			if err != nil {
				it.handleFailedMsg(msg, err)
			} else {
				it.handleSuccessMsg(msg)
			}

			if it.workerWorkIntervalFunc != nil && remain > 0 {
				// TODO: we should move this msg into waiting queue before it take by next ProcessMsg call?
				time.Sleep(time.Millisecond * time.Duration(remain))
			}

			<-it.sema
		}
	}
}

func (it *Processor) handleFailedMsg(msg IMsg, err error) {
	// TBD.
	return
}

func (it *Processor) handleSuccessMsg(msg IMsg) {
	err := it.broker.Complete(it.ctx, msg)
	if err != nil && it.errLogLimiter.Allow() {
		it.logger.Errorf("queue=%s broker.Complete %v", it.queueName, err)
	}
}
