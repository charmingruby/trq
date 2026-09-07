package queue

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"uuid"
)

var (
	ErrJobAlreadyProcessed = errors.New("job already processed")
	ErrJobNotFound         = errors.New("job not found")
)

type JobStatus string

const (
	JobReady      JobStatus = "ready"
	JobProcessing JobStatus = "processing"
	JobCompleted  JobStatus = "completed"
	JobFailed     JobStatus = "failed"
)

type Queue struct {
	jobs    *list.List
	indexes map[string]*list.Element
	mu      sync.Mutex
	notify  chan struct{}
	closed  bool
}

type Job struct {
	ID       string
	Data     []byte
	Status   JobStatus
	Attempts int
}

func New() *Queue {
	return &Queue{
		jobs:    list.New(),
		indexes: make(map[string]*list.Element),
		notify:  make(chan struct{}, 1),
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
		ID:       id,
		Data:     data,
		Status:   JobReady,
		Attempts: 0,
	}

	el := q.jobs.PushBack(j)

	q.indexes[id] = el

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *Queue) reserve() (*Job, bool) {
	for e := q.jobs.Front(); e != nil; e = e.Next() {
		j := e.Value.(*Job)

		if j.Status != JobReady {
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

func (q *Queue) Fail(ctx context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	el, ok := q.indexes[id]
	if !ok {
		return ErrJobNotFound
	}

	jobCopy := *el.Value.(*Job)

	if jobCopy.Status != JobProcessing {
		return ErrJobAlreadyProcessed
	}

	// TODO: to be used on DLQ
	jobCopy.Status = JobFailed

	q.jobs.Remove(el)
	delete(q.indexes, jobCopy.ID)

	return nil
}
