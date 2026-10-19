package gmq

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/time/rate"
)

// Processor manages queue worker(s) for consuming messages.
type Processor struct {
	ctx     context.Context
	cancel  context.CancelFunc
	workers sync.WaitGroup
	broker  Broker
	// rate limiter to prevent spamming logs with a bunch of errors.
	errLogLimiter *rate.Limiter
	handler       IHandler
	logger        Logger
	restIfNoMsg   time.Duration

	queueName string

	// semaphore is a counting semaphore to ensure the number of active workers does not exceed the limit.
	semaphore              chan struct{}
	workerNum              uint16
	workerWorkIntervalFunc FuncWorkInterval
}

type ProcessorParams struct {
	Ctx         context.Context
	Broker      Broker
	RestIfNoMsg time.Duration
	Handler     IHandler
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

	ctx, cancel := context.WithCancel(params.Ctx)
	return &Processor{
		broker:        params.Broker,
		ctx:           ctx,
		cancel:        cancel,
		errLogLimiter: rate.NewLimiter(rate.Every(3*time.Second), 1),
		handler:       params.Handler,
		logger:        params.Logger,
		restIfNoMsg:   params.RestIfNoMsg,
		semaphore:     make(chan struct{}, params.WorkerNum),

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

	it.workers.Add(1)
	go func() {
		defer it.workers.Done()
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
	case <-it.ctx.Done():
		return
	case it.semaphore <- struct{}{}:
		{
			it.workers.Add(1)
			go func() {
				defer it.workers.Done()
				defer func() { <-it.semaphore }()
				msg, err := it.broker.Dequeue(it.ctx, it.queueName)

				switch {
				case errors.Is(err, ErrNoMsg):
					{
						it.wait(it.restIfNoMsg)
						return
					}
				case err != nil:
					{
						if it.errLogLimiter.Allow() && err != context.Canceled && err != redis.ErrClosed {
							it.logger.Errorf("queue=%s broker.Dequeue %v", it.queueName, err)
						}
						return
					}
				}

				if msg == nil {
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
						it.wait(time.Millisecond * time.Duration(remain))
					}
				}

			}()
		}
	}
}

func (it *Processor) handleFailedMsg(msg IMsg, errFail error) {
	err := it.broker.Fail(it.ctx, msg, errFail)
	if err != nil && !errors.Is(err, context.Canceled) && it.errLogLimiter.Allow() {
		it.logger.Errorf("broker.Fail %v, queue=%s msgId=%s", err, it.queueName, msg.GetId())
	}
}

func (it *Processor) handleSuccessMsg(msg IMsg) {
	err := it.broker.Complete(it.ctx, msg)
	if err != nil && !errors.Is(err, context.Canceled) && it.errLogLimiter.Allow() {
		it.logger.Errorf("broker.Complete %v, queue=%s msgId=%s", err, it.queueName, msg.GetId())
	}
}

func (it *Processor) shutdown() {
	it.cancel()
	it.workers.Wait()
}

func (it *Processor) wait(duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-it.ctx.Done():
	case <-timer.C:
	}
}
