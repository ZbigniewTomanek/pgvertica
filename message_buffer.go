package pgvertica

import (
	"io"
	"net"
	"reflect"

	"github.com/jackc/pgproto3/v2"
)

type MessageBufferInterface interface {
	queueMessages(messages ...pgproto3.Message)
	sendQueuedMessages() error
	buffSize() int
}

type MessageBuffer struct {
	conn   net.Conn
	buffer []pgproto3.Message
}

func newMessagesBuffer(conn net.Conn) *MessageBuffer {
	return &MessageBuffer{
		conn:   conn,
		buffer: make([]pgproto3.Message, 0),
	}
}

func (mb *MessageBuffer) queueMessages(messages ...pgproto3.Message) {
	mb.buffer = append(mb.buffer, messages...)
}

func (mb *MessageBuffer) sendQueuedMessages() error {
	if err := writeMessages(mb.conn, mb.buffer...); err != nil {
		return err
	}
	mb.buffer = make([]pgproto3.Message, 0)
	return nil
}

func (mb *MessageBuffer) buffSize() int {
	return len(mb.buffer)
}

func writeMessages(w io.Writer, msgs ...pgproto3.Message) error {
	var buf []byte
	for _, msg := range msgs {
		Logger.Debug("[send] message", "type", reflect.TypeOf(msg), "message", msg)
		buf = msg.Encode(buf)
	}
	_, err := w.Write(buf)
	return err
}
