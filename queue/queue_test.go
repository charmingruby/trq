package queue_test

import (
	"encoding/json"
	"errors"
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
		assert.Equal(t, kind, job.Kind)
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

		err = q.Complete(ctx, job.ID)
		require.NoError(t, err)
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

		err = q.Complete(ctx, job.ID)
		assert.Nil(t, err)

		err = q.Complete(ctx, job.ID)
		require.Error(t, err)
		assert.ErrorIs(t, err, queue.ErrSameJobStatus)
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

		err = q.Fail(ctx, job.ID)
		require.NoError(t, err)
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

		err = q.Fail(ctx, job.ID)
		require.NoError(t, err)

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

		kind := "dummy_message"

		data, err := json.Marshal(dummy)

		require.NoError(t, err)

		ctx := t.Context()

		q.Enqueue(
			ctx,
			kind,
			data,
		)

		err = q.Fail(ctx, "invalid_id")
		require.Error(t, err)
		assert.ErrorIs(t, err, queue.ErrJobNotFound)
	})
}
