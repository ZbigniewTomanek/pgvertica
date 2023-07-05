package pgvertica

import (
	"context"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)

func TestServeConnStartup_HandleStartupMessage_Success(t *testing.T) {
	ctx := context.Background()
	config := ServerConfig{
		PostgresConnectionString: "postgres://test:test@localhost:5432/test?sslmode=disable",
		VerticaConnectionString:  "vertica://test:test@localhost:5433/test?sslmode=disable",
	}
	c := Conn{
		Conn: &MockConn{},
	}
	mockReceiver := new(MockReceiver)
	mockReceiver.On("ReceiveStartupMessage").Return(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"database": "test"},
	}, nil)
	c.receiver = mockReceiver

	_, err := serveConnStartup(ctx, &c, &config)

	assert.NoError(t, err)
}

func TestHandleStartupMessage_Success(t *testing.T) {
	ctx := context.Background()
	config := ServerConfig{
		PostgresConnectionString: "postgres://test:test@localhost:5432/test?sslmode=disable",
		VerticaConnectionString:  "vertica://test:test@localhost:5433/test?sslmode=disable",
	}
	c := Conn{
		Conn: &MockConn{},
	}
	mockReceiver := new(MockReceiver)
	msg := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"database": "test"},
	}
	mockReceiver.On("ReceiveStartupMessage").Return(msg, nil)
	c.receiver = mockReceiver

	err := handleStartupMessage(ctx, &c, msg, &config)

	assert.NoError(t, err)
}
