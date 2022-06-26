package gmq

import (
	"context"
	"time"
)

type Scheduler struct {
	ctx      context.Context
	broker   Broker
	logger   Logger
	location *time.Location
}

type SchedulerParams struct {
	ctx      context.Context
	broker   Broker
	logger   Logger
	location *time.Location
}

func NewScheduler(parmas SchedulerParams) *Scheduler {

	loc, err := time.LoadLocation("UTC")
	if err != nil {
		panic(`LoadLocation("UTC") fail`)
	}

	if parmas.location == nil {
		parmas.location = loc
	}

	return &Scheduler{
		ctx:      parmas.ctx,
		broker:   parmas.broker,
		logger:   parmas.logger,
		location: parmas.location,
	}
}

func (it *Scheduler) Register(cronspec string, msg IMsg, opts ...OptionQueue) (entryId string, err error) {
	// TBD.
	return
}

func (s *Scheduler) Unregister(r string) (err error) {
	// TBD.
	return
}

func (s *Scheduler) Run() (err error) {
	// TBD.
	return
}

func (s *Scheduler) Start() (err error) {
	// TBD.
	return
}
