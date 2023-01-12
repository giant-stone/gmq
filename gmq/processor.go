package gmq

import (
	"context"
	"errors"
	"runtime"
	"time"

	"golang.org/x/time/rate"
)

// Processor manages queue worker(s) for consuming messages.
type Processor struct {
	ctx    context.Context
	broker Broker
	// rate limiter to prevent spamming logs with a bunch of errors.
	errLogLimiter *rate.Limiter
	handler       Handler
	logger        Logger
	restIfNoMsg   time.Duration

	queueName string

	// sema is a counting semaphore to ensure the number of active workers does not exceed the limit.
	sema                   chan struct{}
	workerNum              uint16
	workerWorkIntervalFunc FuncWorkInterval
}

type ProcessorParams struct {
	Ctx         context.Context
	Broker      Broker
	RestIfNoMsg time.Duration
	Handler     Handler
	Logger      Logger
	QueueName   string

	WorkerNum              uint16
	WorkerWorkIntervalFunc FuncWorkInterval
}

func NewProcessor(params ProcessorParams) *Processor {
	if params.WorkerNum == 0 {
		params.WorkerNum = uint16(runtime.NumCPU())
	}

	if params.QueueName == "" {
		params.QueueName = DefaultQueueName
	}

	if params.RestIfNoMsg == 0 {
		params.RestIfNoMsg = time.Second
	}

	return &Processor{
		broker:        params.Broker,
		ctx:           params.Ctx,
		errLogLimiter: rate.NewLimiter(rate.Every(3*time.Second), 1),
		handler:       params.Handler,
		logger:        params.Logger,
		restIfNoMsg:   params.RestIfNoMsg,
		sema:          make(chan struct{}, params.WorkerNum),

		workerNum:              params.WorkerNum,
		queueName:              params.QueueName,
		workerWorkIntervalFunc: params.WorkerWorkIntervalFunc,
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
					return
				}
			default:
				it.exec()
			}
		}
	}()
}

func (it *Processor) exec() {
	select {
	case it.sema <- struct{}{}:
		{
			go func() {
				msg, err := it.broker.Dequeue(it.ctx, it.queueName)

				switch {
				case errors.Is(err, ErrNoMsg):
					{
						time.Sleep(it.restIfNoMsg)
						<-it.sema
						return
					}
				case err != nil:
					{
						if it.errLogLimiter.Allow() && err != context.Canceled {
							it.logger.Errorf("queue=%s broker.Dequeue %v", it.queueName, err)
						}
						<-it.sema
						return
					}
				}

				if msg == nil {
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

				if err != context.DeadlineExceeded {
					if it.workerWorkIntervalFunc != nil && remain > 0 {
						// TODO: we should move this msg into waiting queue before it taken by next ProcessMsg call?
						time.Sleep(time.Millisecond * time.Duration(remain))
					}
				}

				<-it.sema
			}()
		}
	}
}

func (it *Processor) handleFailedMsg(msg IMsg, errFail error) {
	err := it.broker.Fail(it.ctx, msg, errFail)
	if errFail != err && it.errLogLimiter.Allow() {
		it.logger.Errorf("queue:%s op:broker.Fail error(%v)", it.queueName, err)
	}
}

func (it *Processor) handleSuccessMsg(msg IMsg) {
	err := it.broker.Complete(it.ctx, msg)
	if err != nil && it.errLogLimiter.Allow() {
		it.logger.Errorf("queue:%s op:broker.Complete error(%v)", it.queueName, err)
	}
}

func (it *Processor) shutdown() {
}
