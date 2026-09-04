package queue_test

import (
	"encoding/json/v2"
	"testing"

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

		kind := "dummy_message"

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			kind,
			data,
		)

		job, ok := q.Reserve(ctx)

		assert.True(t, ok)

		assert.NotZero(t, job.ID)
		assert.NotZero(t, kind, job.Kind)
		assert.NotZero(t, job.Data)
		assert.Equal(t, 1, job.Attempts)

		var u dummyData
		err = json.Unmarshal(job.Data, &u)
		require.NoError(t, err)

		assert.Equal(t, dummy.Owner, u.Owner)
		assert.Equal(t, dummy.Content, u.Content)
	})
}

func TestQueueComplete(t *testing.T) {
	t.Run("it should complete a job", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		kind := "dummy_message"

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			kind,
			data,
		)

		job, ok := q.Reserve(ctx)
		assert.True(t, ok)

		ok = q.Complete(ctx, job.ID)
		assert.True(t, ok)
	})

	t.Run("it should not complete a job with status different from processing", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		kind := "dummy_message"

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			kind,
			data,
		)

		job, ok := q.Reserve(ctx)
		assert.True(t, ok)

		ok = q.Complete(ctx, job.ID)
		assert.True(t, ok)

		ok = q.Complete(ctx, job.ID)
		assert.False(t, ok)
	})

	t.Run("it should not complete a job with nonexistent id", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		kind := "dummy_message"

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			kind,
			data,
		)

		ok := q.Complete(ctx, "invalid_id")
		assert.False(t, ok)
	})
}

func TestQueueFail(t *testing.T) {
	t.Run("it should fail a job", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		kind := "dummy_message"

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			kind,
			data,
		)

		job, ok := q.Reserve(ctx)
		assert.True(t, ok)

		ok = q.Fail(ctx, job.ID)
		assert.True(t, ok)
	})

	t.Run("it should not fail a job with status different from processing", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		kind := "dummy_message"

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			kind,
			data,
		)

		job, ok := q.Reserve(ctx)
		assert.True(t, ok)

		ok = q.Fail(ctx, job.ID)
		assert.True(t, ok)

		ok = q.Fail(ctx, job.ID)
		assert.False(t, ok)
	})

	t.Run("it should not fail a job with nonexistent id", func(t *testing.T) {
		q := queue.New()

		dummy := dummyData{
			Owner:   "john doe",
			Content: "hey!",
		}

		kind := "dummy_message"

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			kind,
			data,
		)

		ok := q.Fail(ctx, "invalid_id")
		assert.False(t, ok)
	})
}
