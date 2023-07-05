package pgvertica

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockListener struct {
}

func (m *MockListener) Accept() (net.Conn, error) {
	return &MockConn{}, nil
}

func (m *MockListener) Close() error {
	return nil
}

func (m *MockListener) Addr() net.Addr {
	return &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 0,
	}
}

func mockNewConn(conn net.Conn) *Conn {
	return &Conn{
		Conn:     &MockConn{},
		receiver: &MockReceiver{},
	}
}

func mockServer() *Server {
	s := Server{
		conns: make(map[*Conn]struct{}),
		config: &ServerConfig{
			Addr:                     "localhost:5433",
			PostgresConnectionString: "postgres://postgres@localhost:5432/postgres?sslmode=disable",
			VerticaConnectionString:  "vertica://dbadmin@localhost:5433/docker?sslmode=disable",
		},
		ln: &MockListener{},
		listen: func(string, string) (net.Listener, error) {
			return &MockListener{}, nil
		},
		newConn: mockNewConn,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.newConn = func(conn net.Conn) *Conn {
		mockReceiver := new(MockReceiver)
		mockReceiver.On("ReceiveStartupMessage").Return(&pgproto3.StartupMessage{}, nil)
		mockReceiver.On("Receive").Return(&pgproto3.Sync{}, nil)
		return &Conn{
			Conn:     conn,
			receiver: mockReceiver,
		}
	}
	return &s
}

func TestServerOpen(t *testing.T) {
	s := mockServer()
	err := s.Open()
	require.NoError(t, err)

	require.NoError(t, err)
}

func TestServerClose(t *testing.T) {
	s := mockServer()
	s.ctx = context.Background()

	go func() {
		require.NoError(t, s.Open())
	}()
	require.NoError(t, s.Close())
}

func TestServerCloseClientConnections(t *testing.T) {
	s := mockServer()
	require.NoError(t, s.Open())

	s.conns[newConn(&MockConn{})] = struct{}{}
	require.NoError(t, s.CloseClientConnections())

	// conns map should be empty after closing connections.
	assert.Empty(t, s.conns)
}

func TestServerServeConn(t *testing.T) {
	mockReceiver := new(MockReceiver)
	mockReceiver.On("ReceiveStartupMessage").Return(&pgproto3.StartupMessage{}, nil)
	mockReceiver.On("Receive").Return(&pgproto3.Sync{}, nil)
	mockConn := &Conn{
		Conn:     &MockConn{},
		receiver: mockReceiver,
	}
	s := mockServer()
	s.newConn = func(conn net.Conn) *Conn {
		return mockConn
	}

	go func() {
		require.NoError(t, s.Open())

	}()
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	s.ctx = ctx
	s.cancel = cancel
	defer cancel()

	go func() {
		err := s.serveConn(ctx, mockConn)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}()
}
