package queue

import (
	"container/list"
	"context"
	"sync"
	"uuid"
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
}

type Job struct {
	ID       string
	Kind     string
	Data     []byte
	status   JobStatus
	Attempts int
}

func New() *Queue {
	return &Queue{
		jobs:    list.New(),
		indexes: make(map[string]*list.Element),
	}
}

func (q *Queue) Enqueue(ctx context.Context, kind string, data []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()

	id := uuid.NewV7().String()

	j := &Job{
		ID:     id,
		Kind:   kind,
		Data:   data,
		status: JobReady,

		Attempts: 0,
	}

	el := q.jobs.PushBack(j)

	q.indexes[id] = el
}

func (q *Queue) Reserve(ctx context.Context) (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for e := q.jobs.Front(); e != nil; e = e.Next() {
		j := e.Value.(*Job)

		if j.status != JobReady {
			continue
		}

		j.Attempts++
		j.status = JobProcessing

		return *j, true
	}

	return Job{}, false
}

func (q *Queue) Complete(ctx context.Context, id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	el, ok := q.indexes[id]
	if !ok {
		return false
	}

	job := el.Value.(*Job)

	job.status = JobCompleted

	return true
}

func (q *Queue) Fail(ctx context.Context, id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	el, ok := q.indexes[id]
	if !ok {
		return false
	}

	job := el.Value.(*Job)

	job.status = JobFailed

	return true
}
