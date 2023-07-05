package pgvertica

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgproto3/v2"
)

type ParsedConnString struct {
	url  *url.URL
	host string
	port int
}

func parseConnectionString(connStr string) (*ParsedConnString, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		Logger.Error("Error parsing connection string:", "error", err)
		return nil, err
	}

	var host, port string
	if host, port, err = net.SplitHostPort(u.Host); err != nil {
		Logger.Error("Error on split host and port", "error", err)
		return nil, err
	}

	portNum, err := strconv.Atoi(port)
	if err != nil {
		Logger.Error("Error parsing port number", "error", err)
		return nil, err
	}

	return &ParsedConnString{
		url:  u,
		host: host,
		port: portNum,
	}, nil
}

type DBOpener interface {
	Open(driverName, dataSourceName string) (*sql.DB, error)
}

type RealDBOpener struct{}

func (ro RealDBOpener) Open(driverName, dataSourceName string) (*sql.DB, error) {
	return sql.Open(driverName, dataSourceName)
}

type Receiver interface {
	Receive() (pgproto3.FrontendMessage, error)
	ReceiveStartupMessage() (pgproto3.FrontendMessage, error)
}

type BackendWrapper struct {
	backend *pgproto3.Backend
}

func (bw *BackendWrapper) Receive() (pgproto3.FrontendMessage, error) {
	return bw.backend.Receive()
}

func (bw *BackendWrapper) ReceiveStartupMessage() (pgproto3.FrontendMessage, error) {
	return bw.backend.ReceiveStartupMessage()
}

type Conn struct {
	net.Conn
	receiver Receiver
	vdb      *sql.DB
	pgdb     *sql.DB
}

func newConn(conn net.Conn) *Conn {
	return &Conn{
		Conn: conn,
		receiver: &BackendWrapper{
			backend: pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn),
		},
	}
}

func (c *Conn) Close() (err error) {
	if c.vdb != nil {
		if e := c.vdb.Close(); err == nil {
			err = e
		}
	}

	if c.pgdb != nil {
		if e := c.pgdb.Close(); err == nil {
			err = e
		}
	}

	if e := c.Conn.Close(); err == nil {
		err = e
	}
	return err
}

func getDBNameFromConnString(connStr string) (string, error) {
	parsedConn, err := parseConnectionString(connStr)
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(parsedConn.url.Path, "/", ""), nil
}

func buildConnectionString(scheme string, params map[string]string, host string, port int) string {
	user := params["user"]
	password := params["password"]
	database := params["database"]
	var query = url.URL{
		Scheme: scheme,
		User:   url.UserPassword(user, password),
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   database,
	}
	return query.String()
}

func connectToDB(opener DBOpener, driverName string, connectionString string, params map[string]string) (*sql.DB, error) {
	if params != nil {
		parsedConn, err := parseConnectionString(connectionString)
		if err != nil {
			return nil, err
		}
		connectionString = buildConnectionString(parsedConn.url.Scheme, params, parsedConn.host, parsedConn.port)
	}

	Logger.Debug("Connect to DB", "connection", connectionString)
	var db *sql.DB
	var err error
	if db, err = opener.Open(driverName, connectionString); err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}
