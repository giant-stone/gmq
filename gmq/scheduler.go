package gmq

import (
	"context"
	"sync"
	"time"

	"github.com/giant-stone/go/glogging"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

type FuncEnqueueErrorHandler func(msg IMsg, opts []OptionClient, err error)

type Scheduler struct {
	ctx        context.Context
	client     *Client
	cron       *cron.Cron
	errHandler FuncEnqueueErrorHandler
	logger     Logger

	location         *time.Location
	lock             sync.Mutex
	jobId2EntryIdMap map[string]cron.EntryID
}

type SchedulerParams struct {
	Ctx      context.Context
	Broker   Broker
	Logger   Logger
	Location *time.Location

	EnqueueErrorHandler FuncEnqueueErrorHandler
}

func NewScheduler(parmas SchedulerParams) *Scheduler {
	if parmas.Logger == nil {
		parmas.Logger = glogging.Sugared
	}

	if parmas.Location == nil {
		loc, err := time.LoadLocation("UTC")
		if err != nil {
			parmas.Logger.Fatal(`LoadLocation("UTC") fail`)
		}
		parmas.Location = loc
	}

	client, err := NewClientFromBroker(parmas.Broker)
	if err != nil {
		parmas.Logger.Fatalf(`NewClientFromBroker %v`, err)
	}

	return &Scheduler{
		ctx:              parmas.Ctx,
		cron:             cron.New(cron.WithLocation(parmas.Location)),
		client:           client,
		logger:           parmas.Logger,
		location:         parmas.Location,
		jobId2EntryIdMap: make(map[string]cron.EntryID),
		errHandler:       parmas.EnqueueErrorHandler,
	}
}

type CronJob struct {
	ctx        context.Context
	client     *Client
	errHandler FuncEnqueueErrorHandler
	logger     Logger
	opts       []OptionClient
	msg        IMsg
	id         string
}

func (it *CronJob) Run() {
	it.logger.Debugf("CronJob.Run id=%s now=%s", it.id, time.Now().String()[:19])

	_, err := it.client.Enqueue(it.ctx, it.msg, it.opts...)
	if err != nil {
		if it.errHandler != nil {
			it.errHandler(it.msg, it.opts, err)
		}
	}
}

func (it *Scheduler) Register(cronSpec string, msg IMsg, opts ...OptionClient) (jobId string, err error) {
	msgId := msg.GetId()
	if msgId != "" {
		jobId = msgId
	} else {
		jobId = uuid.NewString()
	}

	job := CronJob{
		ctx:    it.ctx,
		client: it.client,
		logger: it.logger,
		msg:    msg,
		opts:   opts,
		id:     jobId,
	}

	entryId, err := it.cron.AddJob(cronSpec, &job)
	if err != nil {
		return "", err
	}

	it.lock.Lock()
	defer it.lock.Unlock()

	it.jobId2EntryIdMap[job.id] = entryId
	return jobId, nil
}

func (it *Scheduler) Unregister(jobId string) (err error) {
	it.lock.Lock()
	defer it.lock.Unlock()

	entryId, ok := it.jobId2EntryIdMap[jobId]
	if ok {
		it.cron.Remove(entryId)
		delete(it.jobId2EntryIdMap, jobId)
	}

	return
}

func (it *Scheduler) Run() (err error) {
	it.cron.Start()
	return nil
}

func (it *Scheduler) Shutdown() {
	if it.cron != nil {
		it.cron.Stop()
	}

	it.logger.Debug("Scheduler stopped")
}
