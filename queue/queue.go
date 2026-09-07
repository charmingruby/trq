package queue

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"uuid"
)

var (
	ErrJobAlreadyProcessed = errors.New("job already processed")
	ErrJobNotFound         = errors.New("job not found")
	ErrMaxRetriesExceeded  = errors.New("max retries exceeded, job moved to DLQ")
)

type JobStatus string

const (
	JobReady      JobStatus = "ready"
	JobProcessing JobStatus = "processing"
	JobCompleted  JobStatus = "completed"
	JobFailed     JobStatus = "failed"
)

type Config struct {
	MaxRetries int
	BaseDelay  time.Duration
	DLQ        *Queue
}

type Option = func(*Config)

func WithMaxRetries(n int) Option {
	return func(c *Config) {
		c.MaxRetries = n
	}
}

func WithBaseDelay(d time.Duration) Option {
	return func(c *Config) {
		c.BaseDelay = d
	}
}

func WithDLQ(q *Queue) Option {
	return func(c *Config) {
		c.DLQ = q
	}
}

type Queue struct {
	jobs    *list.List
	indexes map[string]*list.Element
	mu      sync.Mutex
	notify  chan struct{}
	closed  bool
	config  Config
}

type Job struct {
	ID        string
	Data      []byte
	Status    JobStatus
	Attempts  int
	ReadyAt   time.Time
	CreatedAt time.Time
}

func New(opts ...Option) *Queue {
	cfg := Config{
		MaxRetries: 0,
		BaseDelay:  0,
		DLQ:        nil,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return &Queue{
		jobs:    list.New(),
		indexes: make(map[string]*list.Element),
		notify:  make(chan struct{}, 1),
		config:  cfg,
	}
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}

	q.closed = true
	close(q.notify)
}

func (q *Queue) Enqueue(ctx context.Context, data []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()

	id := uuid.NewV7().String()

	j := &Job{
		ID:        id,
		Data:      data,
		Status:    JobReady,
		Attempts:  0,
		ReadyAt:   time.Time{},
		CreatedAt: time.Now(),
	}

	el := q.jobs.PushBack(j)

	q.indexes[id] = el

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *Queue) reserve() (*Job, bool) {
	now := time.Now()

	for e := q.jobs.Front(); e != nil; e = e.Next() {
		j := e.Value.(*Job)

		if j.Status != JobReady {
			continue
		}

		if !j.ReadyAt.IsZero() && now.Before(j.ReadyAt) {
			continue
		}

		j.Attempts++
		j.Status = JobProcessing

		return j, true
	}

	return nil, false
}

func (q *Queue) Reserve(ctx context.Context) (*Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.reserve()
}

func (q *Queue) WaitReserve(ctx context.Context) (*Job, bool) {
	for {
		q.mu.Lock()

		if q.closed {
			q.mu.Unlock()
			return nil, false
		}

		if job, ok := q.reserve(); ok {
			q.mu.Unlock()
			return job, true
		}

		q.mu.Unlock()

		select {
		case <-q.notify:
		case <-ctx.Done():
			return nil, false
		}
	}
}

func (q *Queue) Complete(ctx context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	el, ok := q.indexes[id]
	if !ok {
		return ErrJobNotFound
	}

	job := el.Value.(*Job)

	if job.Status != JobProcessing {
		return ErrJobAlreadyProcessed
	}

	job.Status = JobCompleted

	return nil
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.jobs.Len()
}

func (q *Queue) Fail(ctx context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	el, ok := q.indexes[id]
	if !ok {
		return ErrJobNotFound
	}

	job := el.Value.(*Job)

	if job.Status != JobProcessing {
		return ErrJobAlreadyProcessed
	}

	if q.config.MaxRetries > 0 && job.Attempts < q.config.MaxRetries {
		delay := q.config.BaseDelay
		for i := 1; i < job.Attempts; i++ {
			delay *= 2
		}

		job.ReadyAt = time.Now().Add(delay)
		job.Status = JobReady

		return nil
	}

	q.jobs.Remove(el)
	delete(q.indexes, job.ID)

	if q.config.DLQ != nil {
		job.Status = JobFailed
		job.ReadyAt = time.Time{}

		dlqEl := q.config.DLQ.jobs.PushBack(job)
		q.config.DLQ.indexes[job.ID] = dlqEl
	}

	return ErrMaxRetriesExceeded
}
