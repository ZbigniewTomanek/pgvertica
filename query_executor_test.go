package pgvertica

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockConn struct {
	net.Conn
	buf            bytes.Buffer
	RemoteAddrFunc func() net.Addr
	err            error
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.buf.Write(b)
}

func (c *MockConn) Close() error {
	return nil
}

func (c *MockConn) RemoteAddr() net.Addr {
	if c.RemoteAddrFunc != nil {
		return c.RemoteAddrFunc()
	}
	return &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 0,
	}
}
func (mc *MockConn) LocalAddr() net.Addr                { return nil }
func (mc *MockConn) SetDeadline(t time.Time) error      { return nil }
func (mc *MockConn) SetReadDeadline(t time.Time) error  { return nil }
func (mc *MockConn) SetWriteDeadline(t time.Time) error { return nil }
func (mc *MockConn) Read(b []byte) (n int, err error)   { return 0, nil }

type MockReceiver struct {
	mock.Mock
}

func (m *MockReceiver) Receive() (pgproto3.FrontendMessage, error) {
	args := m.Called()
	return args.Get(0).(pgproto3.FrontendMessage), args.Error(1)
}

func (m *MockReceiver) ReceiveStartupMessage() (pgproto3.FrontendMessage, error) {
	args := m.Called()
	return args.Get(0).(pgproto3.FrontendMessage), args.Error(1)
}

type MockMessageBuffer struct {
}

func (m *MockMessageBuffer) queueMessages(messages ...pgproto3.Message) {
}

func (m *MockMessageBuffer) sendQueuedMessages() error {
	return nil
}

func (m *MockMessageBuffer) buffSize() int {
	return 0
}

func newMockedQueryExecutor() QueryExecutor {
	return QueryExecutor{
		ctx:                context.Background(),
		conn:               &Conn{Conn: &MockConn{}, receiver: nil, vdb: nil, pgdb: nil},
		preparedStatements: make(map[string]*PreparedStatement),
		queryUtil:          mockedQueryUtil(),
		mb:                 &MockMessageBuffer{},
		inTransaction:      false,
	}

}

func TestGetTransactionStatus(t *testing.T) {
	t.Run("Transaction in progress", func(t *testing.T) {
		qe := QueryExecutor{
			ctx:                context.Background(),
			conn:               &Conn{},
			preparedStatements: make(map[string]*PreparedStatement),
			inTransaction:      true,
		}

		assert.Equal(t, byte('T'), qe.getTransactionStatus())
	})

	t.Run("No transaction in progress", func(t *testing.T) {
		qe := QueryExecutor{
			ctx:                context.Background(),
			conn:               &Conn{},
			preparedStatements: make(map[string]*PreparedStatement),
			inTransaction:      false,
		}

		assert.Equal(t, byte('I'), qe.getTransactionStatus())
	})
}

func TestHandleBindMessage(t *testing.T) {
	t.Run("PreparedStatement does not exist", func(t *testing.T) {
		qe := newMockedQueryExecutor()

		bmsg := &pgproto3.Bind{
			PreparedStatement: "does_not_exist",
		}

		err := qe.handleBindMessage(bmsg)
		assert.NotNil(t, err)
	})

	t.Run("PreparedStatement exists", func(t *testing.T) {
		qe := newMockedQueryExecutor()
		mockReceiver := new(MockReceiver)
		mockReceiver.On("Receive").Return(&pgproto3.Sync{}, nil).Once()

		c := &Conn{
			pgdb:     nil,
			vdb:      nil,
			Conn:     &MockConn{},
			receiver: mockReceiver,
		}
		qe.conn = c
		qe.preparedStatements = map[string]*PreparedStatement{
			"exists": {
				name:          "exists",
				query:         "SELECT * FROM users WHERE id = ?",
				parameterOIDs: []uint32{0},
			},
		}
		bmsg := &pgproto3.Bind{
			PreparedStatement:    "exists",
			Parameters:           [][]byte{[]byte("1111")},
			ParameterFormatCodes: []int16{1},
		}

		err := qe.handleBindMessage(bmsg)
		assert.Nil(t, err)
	})
}

func TestHandleQueryMessage(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to open mock sql db: %v", err)
	}
	defer db.Close()

	qe := newMockedQueryExecutor()
	qe.conn.pgdb = db
	qe.conn.vdb = db

	query := "SELECT * FROM test"
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow("1", "Test")

	mock.ExpectQuery("SELECT ").WillReturnRows(rows)

	msg := &pgproto3.Query{String: query}
	err = qe.handleQueryMessage(msg)
	assert.NoError(t, err)
}

func TestHandlePreparedStatement__HandleBind(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	qe := newMockedQueryExecutor()

	mockReceiver := new(MockReceiver)
	mockReceiver.On("Receive").Return(&pgproto3.Bind{}, nil).Once().On("Receive").Return(&pgproto3.Sync{}, nil).Once()

	c := &Conn{
		pgdb:     db,
		vdb:      db,
		Conn:     &MockConn{},
		receiver: mockReceiver,
	}
	qe.conn = c

	ps := &PreparedStatement{
		name:          "test_name",
		query:         "SELECT 1",
		parameterOIDs: []uint32{},
		parameters:    nil,
		binds:         nil,
	}

	err = qe.handlePreparedStatement(ps)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandlePreparedStatement__HandleDescribe(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mockReceiver := new(MockReceiver)
	mockReceiver.On("Receive").Return(&pgproto3.Describe{ObjectType: 'S'}, nil).Once().On("Receive").Return(&pgproto3.Sync{}, nil).Once()

	c := &Conn{
		pgdb:     db,
		vdb:      db,
		Conn:     &MockConn{},
		receiver: mockReceiver,
	}

	qe := newMockedQueryExecutor()
	qe.conn = c

	ps := &PreparedStatement{
		name:          "test_name",
		query:         "SELECT 1",
		parameterOIDs: []uint32{},
		parameters:    nil,
		binds:         nil,
	}
	mock.ExpectPrepare("SELECT 1").WillReturnError(nil)
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"column"}).AddRow(1))

	err = qe.handlePreparedStatement(ps)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandlePreparedStatement__HandleExecute(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mockReceiver := new(MockReceiver)
	mockReceiver.On("Receive").Return(&pgproto3.Execute{}, nil)

	c := &Conn{
		pgdb:     db,
		vdb:      db,
		Conn:     &MockConn{},
		receiver: mockReceiver,
	}

	qe := newMockedQueryExecutor()
	qe.conn = c
	ps := &PreparedStatement{
		name:          "test_name",
		query:         "SELECT 1",
		parameterOIDs: []uint32{},
		parameters:    nil,
		binds:         nil,
	}
	mock.ExpectPrepare("SELECT 1").WillReturnError(nil)
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"column"}).AddRow("1"))

	err = qe.handlePreparedStatement(ps)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandlePreparedStatement__HandleSync(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mockReceiver := new(MockReceiver)
	mockReceiver.On("Receive").Return(&pgproto3.Sync{}, nil)

	c := &Conn{
		pgdb:     db,
		vdb:      db,
		Conn:     &MockConn{},
		receiver: mockReceiver,
	}
	qe := newMockedQueryExecutor()
	qe.conn = c

	ps := &PreparedStatement{
		name:          "test_name",
		query:         "SELECT 1",
		parameterOIDs: []uint32{},
		parameters:    nil,
		binds:         nil,
	}

	err = qe.handlePreparedStatement(ps)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandlePreparedStatement__HandleClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mockReceiver := new(MockReceiver)
	mockReceiver.On("Receive").Return(&pgproto3.Sync{}, nil)

	c := &Conn{
		pgdb:     db,
		vdb:      db,
		Conn:     &MockConn{},
		receiver: mockReceiver,
	}

	qe := newMockedQueryExecutor()
	qe.conn = c

	ps := &PreparedStatement{
		name:          "test_name",
		query:         "SELECT 1",
		parameterOIDs: []uint32{},
		parameters:    nil,
		binds:         nil,
	}

	err = qe.handlePreparedStatement(ps)
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}
