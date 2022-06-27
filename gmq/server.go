package gmq

import (
	"context"
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

	// pattern name is also queue name
	for queueName := range mux.GetPatterns() {
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
