package workqueue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/charmingruby/trq/journal"
	"github.com/charmingruby/trq/queue"
)

const (
	defaultConcurrency  = 4
	defaultTimeoutInSec = 15
)

var ErrUnableToReserveJob = errors.New("unable to reserve job")

type ProcessingResult struct {
	WorkerID int
	JobID    string
	Status   string
	Err      error
}

type Workqueue struct {
	queue           *queue.Queue
	concurrency     int
	wg              sync.WaitGroup
	resultCh        chan ProcessingResult
	timeoutDuration time.Duration
	journal         *journal.Journal
}

type Handler = func(ctx context.Context, job *queue.Job) error

type Option = func(*Workqueue)

func New(q *queue.Queue, opts ...Option) *Workqueue {
	w := &Workqueue{}

	for _, opt := range opts {
		opt(w)
	}

	w.queue = q
	w.wg = sync.WaitGroup{}
	w.resultCh = make(chan ProcessingResult, 100)

	return w
}

func WithTimeout(duration time.Duration) func(*Workqueue) {
	return func(w *Workqueue) {
		if duration.Nanoseconds() == 0 {
			w.timeoutDuration = defaultTimeoutInSec * time.Second

			return
		}

		w.timeoutDuration = duration
	}
}

func WithJournal(j *journal.Journal) func(*Workqueue) {
	return func(w *Workqueue) {
		w.journal = j
	}
}

func WithConcurrency(c int) func(*Workqueue) {
	return func(w *Workqueue) {
		if c <= 0 {
			w.concurrency = defaultConcurrency

			return
		}

		w.concurrency = c
	}
}

func (w *Workqueue) Process(ctx context.Context, handlerFn Handler) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < w.concurrency; i++ {
		w.wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				job, ok := w.queue.WaitReserve(ctx)
				if !ok {
					return
				}

				if err := handlerFn(ctx, job); err != nil {
					if err := w.queue.Fail(ctx, job.ID); err != nil {
						w.sendResult(i, job, err)
						continue
					}

					w.sendResult(i, job, err)
					continue
				}

				if err := w.queue.Complete(ctx, job.ID); err != nil {
					w.sendResult(i, job, err)
					continue
				}

				w.sendResult(i, job, nil)
			}
		})
	}

	go func() {
		w.wg.Wait()
		close(w.resultCh)
	}()

	if w.journal != nil {
		for r := range w.resultCh {
			e := journal.Entry{
				WorkerID: r.WorkerID,
				JobID:    r.JobID,
				Status:   r.Status,
			}

			if r.Err != nil {
				e.Message = r.Err.Error()
				w.journal.Append(e)
				continue
			}

			w.journal.Append(e)
		}
	}

	return nil
}

func (w *Workqueue) sendResult(workerID int, job *queue.Job, err error) {
	w.resultCh <- ProcessingResult{
		WorkerID: workerID,
		JobID:    job.ID,
		Status:   string(job.Status),
		Err:      err,
	}
}
