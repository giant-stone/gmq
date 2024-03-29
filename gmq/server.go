package gmq

import (
	"context"
	"fmt"
	"time"

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

	if cfg.MsgMaxTTL == 0 {
		cfg.MsgMaxTTL = time.Second * time.Duration(TTLMsg)
	}
	if cfg.RestIfNoMsg == 0 {
		cfg.RestIfNoMsg = DefaultDurationRestIfNoMsg
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
			Ctx:         it.ctx,
			Broker:      it.broker,
			Handler:     mux,
			Logger:      it.logger,
			RestIfNoMsg: it.cfg.RestIfNoMsg,
			QueueName:   queueName,
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
		Broker:     it.broker,
		Ctx:        it.ctx,
		QueueNames: it.queueNames,
		Logger:     it.logger,
		MsgMaxTTL:  it.cfg.MsgMaxTTL,
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
	if _, has := it.queueNames[qname]; !has {
		return nil
	}
	return it.broker.Pause(it.ctx, qname)
}

func (it *Server) Resume(qname string) error {
	if _, has := it.queueNames[qname]; !has {
		return nil
	}
	return it.broker.Resume(it.ctx, qname)
}

func (it *Server) Cfg() *Config {
	return it.cfg
}
