package pgvertica

import (
	"fmt"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)

type MockMessage struct {
	pgproto3.Message
}

func (mm *MockMessage) Encode(dst []byte) []byte {
	if dst == nil {
		return make([]byte, 'a')
	} else {
		return append(dst, 'a')
	}
}

func TestQueueMessages(t *testing.T) {
	mb := newMessagesBuffer(&MockConn{})
	mb.queueMessages(&MockMessage{})
	assert.Equal(t, 1, mb.buffSize(), "The buffer size should be 1 after adding one message")
}

func TestSendQueuedMessages(t *testing.T) {
	{
		mb := newMessagesBuffer(&MockConn{})
		mb.queueMessages(&MockMessage{})

		err := mb.sendQueuedMessages()
		assert.NoError(t, err, "Should not return error on happy path")
		assert.Equal(t, 0, mb.buffSize(), "The buffer should be empty after sending messages")
	}

	{
		mb := newMessagesBuffer(&MockConn{})
		mb.queueMessages(&MockMessage{})
		mb.conn.(*MockConn).err = fmt.Errorf("error")

		err := mb.sendQueuedMessages()
		assert.Error(t, err, "Should return error if Write returns error")
	}
}

func TestWriteMessages(t *testing.T) {
	{
		err := writeMessages(&MockConn{}, &MockMessage{})

		assert.NoError(t, err, "Should not return error on happy path")
	}

	{
		err := writeMessages(&MockConn{err: fmt.Errorf("error")}, &MockMessage{})

		assert.Error(t, err, "Should return error if Write returns error")
	}
}
