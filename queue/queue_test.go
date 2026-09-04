package queue_test

import (
	"encoding/json/v2"
	"testing"

	"github.com/charmingruby/trq/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueReserve(t *testing.T) {
	type message struct {
		Owner   string `json:"owner"`
		Content string `json:"content"`
	}

	t.Run("it should return a enqueued job", func(t *testing.T) {
		q := queue.New()

		dummyMsg := message{
			Owner:   "john doe",
			Content: "hey!",
		}

		kind := "dummy_message"

		data, err := json.Marshal(dummyMsg)

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

		var u message
		err = json.Unmarshal(job.Data, &u)
		require.NoError(t, err)

		assert.Equal(t, dummyMsg.Owner, u.Owner)
		assert.Equal(t, dummyMsg.Content, u.Content)
	})
}
