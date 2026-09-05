package workqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/charmingruby/trq/queue"
)

const defaultConcurrency = 4

var ErrUnableToReserveJob = errors.New("unable to reserve job")

type ProcessingResult struct {
	WorkerID int
	JobID    string
	Err      error
}

type Workqueue struct {
	queue           *queue.Queue
	concurrency     int
	wg              sync.WaitGroup
	resultCh        chan ProcessingResult
	timeoutDuration time.Duration
}

type Handler = func(ctx context.Context, job queue.Job) error

func New(q *queue.Queue, concurrency int, timeoutDuration time.Duration) *Workqueue {
	c := concurrency
	if c <= 0 {
		c = defaultConcurrency
	}

	return &Workqueue{
		queue:           q,
		concurrency:     c,
		wg:              sync.WaitGroup{},
		resultCh:        make(chan ProcessingResult, 100),
		timeoutDuration: timeoutDuration,
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

				err := handlerFn(ctx, job)
				if err != nil {
					if failErr := w.queue.Fail(ctx, job.ID); failErr != nil {
						w.sendResult(i, job, failErr)
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

	for r := range w.resultCh {
		msg := "processed successfully"
		if r.Err != nil {
			msg = fmt.Sprintf("processing error: %s", r.Err.Error())
		}

		fmt.Printf("[%s] Worker %d (%s): %s\n",
			time.Now().String(),
			r.WorkerID,
			r.JobID,
			msg,
		)
	}

	return nil
}

func (w *Workqueue) sendResult(workerID int, job queue.Job, err error) {
	w.resultCh <- ProcessingResult{
		WorkerID: workerID,
		JobID:    job.ID,
		Err:      err,
	}
}
