package gmq

import (
	"context"
	"errors"
	"fmt"

	"github.com/giant-stone/go/glogging"
)

type Server struct {
	ctx     context.Context
	cfg     *Config
	cleaner *Cleaner
	broker  Broker
	logger  Logger

	processors map[string]*Processor
	queueNames map[string]struct{}
}

func NewServer(ctx context.Context, b Broker, cfg *Config) *Server {
	if cfg == nil {
		cfg = &Config{}
	}

	var logger Logger
	if cfg.Logger == nil {
		logger = glogging.Sugared
	} else {
		logger = cfg.Logger
	}

	return &Server{
		ctx:        ctx,
		broker:     b,
		logger:     logger,
		cfg:        cfg,
		processors: make(map[string]*Processor),
		queueNames: make(map[string]struct{}),
	}
}

func (it *Server) Run(mux *Mux) (err error) {
	if mux == nil {
		return fmt.Errorf("no handler(s)")
	}

	patterns := mux.GetPatterns()
	if len(patterns) == 0 {
		return fmt.Errorf("no handler(s)")
	}

	// pattern name is also queue name
	for queueName := range patterns {
		queueCfg, ok := it.cfg.QueueCfgs[queueName]
		params := ProcessorParams{
			Ctx:       it.ctx,
			Broker:    it.broker,
			Handler:   mux,
			Logger:    it.logger,
			QueueName: queueName,
		}

		if ok {
			for _, opt := range queueCfg.opts {
				switch opt.Type() {
				case OptTypeServerWorkerNum:
					{
						params.WorkerNum = opt.Value().(uint16)
					}
				case OptTypeServerWorkerWorkIntervalFunc:
					{
						params.WorkerWorkIntervalFunc = opt.Value().(FuncWorkInterval)
					}
				}
			}
		}

		processor := NewProcessor(params)
		it.processors[queueName] = processor

		it.queueNames[queueName] = struct{}{}
	}

	for _, p := range it.processors {
		p.start()
	}

	// auto-delete dead messages
	it.cleaner = NewCleaner(CleanerParams{
		Ctx:        it.ctx,
		Broker:     it.broker,
		QueueNames: it.queueNames,
		Logger:     it.logger,
	})
	it.cleaner.start()

	return nil
}

func (it *Server) Shutdown() {
	for _, p := range it.processors {
		p.shutdown()
	}

	it.broker.Close()
}

func (it *Server) Pause(qname string) error {
	var err error
	if _, has := it.queueNames[qname]; !has {
		it.logger.Debug("Pause failed, invalid queue name")
	}

	if err = it.broker.Pause(qname); err != nil {
		if errors.Is(err, ErrInternal) {
			it.logger.Debug("warning: the queue is already paused")
		} else {
			it.logger.Debugf("fatal: error occurs when pause queue: %s, error(%s)", qname, err)
		}
	}
	return err
}

func (it *Server) Resume(qname string) error {
	var err error
	if _, has := it.queueNames[qname]; !has {
		it.logger.Debug("Resume failed, invalid queue name")
	}

	if err = it.broker.Resume(qname); err != nil {
		if errors.Is(err, ErrInternal) {
			it.logger.Debug("warning: the queue is not paused")
		} else {
			it.logger.Debugf("fatal: error occurs when pause queue: %s, error(%s)", qname, err)
		}
	}
	return err
}
