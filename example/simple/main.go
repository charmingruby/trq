package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/charmingruby/trq/journal"
	"github.com/charmingruby/trq/queue"
	"github.com/charmingruby/trq/workqueue"
)

type sampleData struct {
	Name string `json:"name"`
}

func main() {
	ctx := context.TODO()
	q := queue.New()
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
			fmt.Printf("\t processed: %s\n", job.ID)
			return nil
		})
	}()

	enqueueData(ctx, q, 10000)

	select {}
}

func enqueueData(ctx context.Context, q *queue.Queue, amount int) {
	for i := 0; amount > i; i++ {
		data, _ := json.Marshal(sampleData{Name: "any"})
		q.Enqueue(ctx, data)
	}
}
