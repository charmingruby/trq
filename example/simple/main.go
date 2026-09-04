package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
	wq := workqueue.New(q, 5, to)

	go func() {
		wq.Process(func(ctx context.Context, job queue.Job) error {
			fmt.Printf("\t processed: %s\n", job.ID)
			return nil
		})
	}()

	enqueueData(ctx, q, 100)

	select {}
}

func enqueueData(ctx context.Context, q *queue.Queue, amount int) {
	for i := 0; amount > i; i++ {
		data, _ := json.Marshal(sampleData{Name: "any"})
		q.Enqueue(ctx, data)
	}
}
