package pgvertica

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"reflect"
	"sync"

	"github.com/jackc/pgproto3/v2"
	"golang.org/x/sync/errgroup"

	postgres "github.com/lib/pq"
	vertigo "github.com/vertica/vertica-sql-go"
)

// hack to keep libs imported
var _ = vertigo.VerticaContext.Deadline
var _ = postgres.ConnectorNotificationHandler

// Postgres settings.
const (
	ServerVersion = "15.2.0"
)

const (
	ApplicationName = "PostgresProxy"
)

type ServerConfig struct {
	Addr                     string
	VerticaConnectionString  string
	PostgresConnectionString string
	RequirePassword          bool
	LogLevel                 int
	TlsConfig                *tls.Config
	SynchronizedSchemas      []string
}

type Listener interface {
	Accept() (net.Conn, error)
	Close() error
	Addr() net.Addr
}

type Server struct {
	mu    sync.Mutex
	ln    Listener
	conns map[*Conn]struct{}

	g      errgroup.Group
	ctx    context.Context
	cancel func()

	config  *ServerConfig
	listen  func(string, string) (net.Listener, error)
	newConn func(net.Conn) *Conn
}

func NewServer(config *ServerConfig) *Server {
	s := &Server{
		conns:   make(map[*Conn]struct{}),
		config:  config,
		listen:  net.Listen,
		newConn: newConn,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s
}

func (s *Server) Open() (err error) {
	s.ln, err = s.listen("tcp", s.config.Addr)
	if err != nil {
		return err
	}

	s.g.Go(func() error {
		if err := s.serve(); s.ctx.Err() != nil {
			return err // return error unless context canceled
		}
		return nil
	})
	return nil
}

func (s *Server) Close() (err error) {
	if s.ln != nil {
		if e := s.ln.Close(); err == nil {
			err = e
		}
	}
	s.cancel()

	// Track and close all open connections.
	if e := s.CloseClientConnections(); err == nil {
		err = e
	}

	if err := s.g.Wait(); err != nil {
		return err
	}
	return err
}

// CloseClientConnections disconnects all Postgres connections.
func (s *Server) CloseClientConnections() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for conn := range s.conns {
		if e := conn.Close(); err == nil {
			err = e
		}
	}

	s.conns = make(map[*Conn]struct{})

	return err
}

// CloseClientConnection disconnects a Postgres connections.
func (s *Server) CloseClientConnection(conn *Conn) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.conns, conn)
	return conn.Close()
}

func (s *Server) serve() error {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return err
		}
		conn := s.newConn(c)

		// Track live connections.
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		Logger.Info("connection accepted", "address", conn.RemoteAddr())

		s.g.Go(func() error {
			defer s.CloseClientConnection(conn)

			if err := s.serveConn(s.ctx, conn); err != nil && s.ctx.Err() == nil {
				Logger.Error("connection error, closing", "error", err)
				return nil
			}

			Logger.Info("connection closed", "address", conn.RemoteAddr())
			return nil
		})
	}
}

func (s *Server) serveConn(ctx context.Context, initalConn *Conn) error {
	conn, err := serveConnStartup(ctx, initalConn, s.config)
	if err != nil {
		return fmt.Errorf("startup: %w", err)
	}

	queryExecutor := newQueryExecutor(ctx, conn, s.config)

	for {
		msg, err := conn.receiver.Receive()
		if err != nil {
			return fmt.Errorf("receive message: %w", err)
		}

		Logger.Debug("[recv][s]", "type", reflect.TypeOf(msg), "message", msg)

		switch msg := msg.(type) {
		case *pgproto3.Query:
			if err := queryExecutor.handleQueryMessage(msg); err != nil {
				Logger.Error("query message", "error", err)
			}
		case *pgproto3.Parse:
			if err := queryExecutor.handleParseMessage(msg); err != nil {
				Logger.Error("parse message", "error", err)
			}
		case *pgproto3.Bind:
			if err := queryExecutor.handleBindMessage(msg); err != nil {
				Logger.Error("bind message", "error", err)
			}
		case *pgproto3.Describe:
			if err := queryExecutor.handleDescribe(msg); err != nil {
				Logger.Error("describe message", "error", err)
			}
		case *pgproto3.Close:
			queryExecutor.mb.queueMessages(&pgproto3.CloseComplete{})
		case *pgproto3.Sync:
			queryExecutor.mb.queueMessages(&pgproto3.ReadyForQuery{TxStatus: queryExecutor.getTransactionStatus()})
			if err := queryExecutor.mb.sendQueuedMessages(); err != nil {
				return err
			}
		case *pgproto3.Terminate:
			return nil // exit

		default:
			Logger.Warn("unexpected message type: %#v", msg)
		}
	}
}

func getParameter(m map[string]string, k string) string {
	if m == nil {
		return ""
	}
	return m[k]
}
