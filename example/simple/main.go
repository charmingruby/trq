package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/charmingruby/trq/journal"
	"github.com/charmingruby/trq/queue"
	"github.com/charmingruby/trq/workqueue"
)

type sampleData struct {
	Name    string `json:"name"`
	FailAll bool   `json:"fail_all"`
}

func main() {
	ctx := context.TODO()

	dlq := queue.New()
	q := queue.New(
		queue.WithMaxRetries(3),
		queue.WithBaseDelay(1*time.Second),
		queue.WithDLQ(dlq),
	)
	to := 5 * time.Second

	j, err := journal.New("./tmp/journal")
	if err != nil {
		os.Exit(1)
	}

	wq := workqueue.New(
		q,
		workqueue.WithConcurrency(5),
		workqueue.WithJournal(j),
		workqueue.WithTimeout(to),
	)

	go func() {
		wq.Process(ctx, func(ctx context.Context, job *queue.Job) error {
			fmt.Printf("\t processing: %s (attempt %d)\n", job.ID, job.Attempts)

			var d sampleData
			if err := json.Unmarshal(job.Data, &d); err != nil {
				return err
			}

			if d.FailAll {
				return errors.New("intentional failure")
			}

			return nil
		})
	}()

	enqueueData(ctx, q, 10000)
	q.Close()

	select {}
}

func enqueueData(ctx context.Context, q *queue.Queue, amount int) {
	for i := 0; amount > i; i++ {
		data, _ := json.Marshal(sampleData{Name: "any"})
		q.Enqueue(ctx, data)
	}
}
