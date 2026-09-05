package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/charmingruby/trq/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyData struct {
	Owner   string `json:"owner"`
	Content string `json:"content"`
}

func TestQueueReserve(t *testing.T) {
	t.Run("it should return a enqueued job", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			data,
		)

		job, ok := q.Reserve(ctx)

		assert.True(t, ok)

		assert.NotZero(t, job.ID)
		assert.NotZero(t, job.Data)
		assert.Equal(t, 1, job.Attempts)
		assert.Equal(t, queue.JobProcessing, job.Status)

		var u dummyData
		err = json.Unmarshal(job.Data, &u)
		require.NoError(t, err)

		assert.Equal(t, dummy.Owner, u.Owner)
		assert.Equal(t, dummy.Content, u.Content)
	})
}

func TestQueueWaitReserve(t *testing.T) {
	t.Run("it should return an enqueued job immediately", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		data, err := json.Marshal(dummy)
		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(ctx, data)

		job, ok := q.WaitReserve(ctx)

		assert.True(t, ok)
		assert.NotZero(t, job.ID)
		assert.NotZero(t, job.Data)
		assert.Equal(t, 1, job.Attempts)
		assert.Equal(t, queue.JobProcessing, job.Status)

		var u dummyData
		err = json.Unmarshal(job.Data, &u)
		require.NoError(t, err)

		assert.Equal(t, dummy.Owner, u.Owner)
		assert.Equal(t, dummy.Content, u.Content)
	})

	t.Run("it should block until a job is enqueued", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		data, err := json.Marshal(dummy)
		require.NoError(t, err)

		ctx := t.Context()

		var gotJob queue.Job
		var gotOK bool

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			gotJob, gotOK = q.WaitReserve(ctx)
		}()

		time.Sleep(50 * time.Millisecond)

		q.Enqueue(ctx, data)

		wg.Wait()

		assert.True(t, gotOK)
		assert.NotZero(t, gotJob.ID)
		assert.Equal(t, queue.JobProcessing, gotJob.Status)
	})

	t.Run("it should return false when context is cancelled", func(t *testing.T) {
		q := queue.New()

		ctx, cancel := context.WithCancel(context.Background())

		var gotOK bool

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			_, gotOK = q.WaitReserve(ctx)
		}()

		time.Sleep(50 * time.Millisecond)

		cancel()

		wg.Wait()

		assert.False(t, gotOK)
	})

	t.Run("it should reserve jobs in FIFO order", func(t *testing.T) {
		q := queue.New()

		ctx := t.Context()

		d1, _ := json.Marshal(dummyData{Owner: "first", Content: "1"})
		d2, _ := json.Marshal(dummyData{Owner: "second", Content: "2"})

		q.Enqueue(ctx, d1)
		q.Enqueue(ctx, d2)

		job1, ok := q.WaitReserve(ctx)
		require.True(t, ok)

		job2, ok := q.WaitReserve(ctx)
		require.True(t, ok)

		var u1, u2 dummyData
		require.NoError(t, json.Unmarshal(job1.Data, &u1))
		require.NoError(t, json.Unmarshal(job2.Data, &u2))

		assert.Equal(t, "first", u1.Owner)
		assert.Equal(t, "second", u2.Owner)
	})
}

func TestQueueComplete(t *testing.T) {
	t.Run("it should complete a job", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			data,
		)

		job, ok := q.Reserve(ctx)
		assert.True(t, ok)
		assert.Equal(t, queue.JobProcessing, job.Status)

		err = q.Complete(ctx, job.ID)
		require.NoError(t, err)
	})

	t.Run("it should not complete a job with status different from processing", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			data,
		)

		job, ok := q.Reserve(ctx)
		assert.True(t, ok)
		assert.Equal(t, queue.JobProcessing, job.Status)

		err = q.Complete(ctx, job.ID)
		require.NoError(t, err)

		err = q.Complete(ctx, job.ID)
		require.Error(t, err)
		assert.ErrorIs(t, err, queue.ErrJobAlreadyProcessed)
	})

	t.Run("it should not complete a job with nonexistent id", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			data,
		)

		err = q.Complete(ctx, "invalid_id")
		require.Error(t, err)
		assert.ErrorIs(t, err, queue.ErrJobNotFound)
	})
}

func TestQueueFail(t *testing.T) {
	t.Run("it should fail a job", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			data,
		)

		job, ok := q.Reserve(ctx)
		assert.True(t, ok)
		assert.Equal(t, queue.JobProcessing, job.Status)

		err = q.Fail(ctx, job.ID)
		require.NoError(t, err)
	})

	t.Run("it should not fail a job with status different from processing", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			data,
		)

		job, ok := q.Reserve(ctx)
		assert.True(t, ok)
		assert.Equal(t, queue.JobProcessing, job.Status)

		err = q.Fail(ctx, job.ID)
		require.NoError(t, err)

		_, ok = q.Reserve(ctx)
		assert.False(t, ok)

		err = q.Fail(ctx, job.ID)
		require.Error(t, err)
		assert.True(t, errors.Is(err, queue.ErrJobNotFound))
	})

	t.Run("it should not fail a job with nonexistent id", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			data,
		)

		err = q.Fail(ctx, "invalid_id")
		require.Error(t, err)
		assert.ErrorIs(t, err, queue.ErrJobNotFound)
	})
}
