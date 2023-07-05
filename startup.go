package pgvertica

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/jackc/pgproto3/v2"
)

func serveConnStartup(ctx context.Context, c *Conn, config *ServerConfig) (*Conn, error) {
	msg, err := c.receiver.ReceiveStartupMessage()
	if err != nil {
		return nil, fmt.Errorf("receive startup message: %w", err)
	}

	switch msg := msg.(type) {
	case *pgproto3.StartupMessage:
		if err := handleStartupMessage(ctx, c, msg, config); err != nil {
			return nil, fmt.Errorf("startup message: %w", err)
		}
		return c, nil
	case *pgproto3.SSLRequest:
		conn, err := handleSSLRequestMessage(ctx, c, msg, config)
		if err != nil {
			return conn, fmt.Errorf("ssl request message: %w", err)
		}
		return conn, nil
	default:
		return c, fmt.Errorf("unexpected startup message: %#v", msg)
	}
}

func handleStartupMessage(ctx context.Context, c *Conn, msg *pgproto3.StartupMessage, config *ServerConfig) error {
	Logger.Debug("received startup message", "message", msg)

	vparams := make(map[string]string)

	name := getParameter(msg.Parameters, "database")
	if name == "" {
		return writeMessages(c, &pgproto3.ErrorResponse{Message: "database required"})
	}

	for k, v := range msg.Parameters {
		vparams[k] = v
	}

	if config.RequirePassword {
		if err := writeMessages(c, &pgproto3.AuthenticationCleartextPassword{}); err != nil {
			return err
		}

		pmsg, err := c.receiver.Receive()
		if err != nil {
			return err
		}

		switch pmsg := pmsg.(type) {
		case *pgproto3.PasswordMessage:
			vparams["password"] = pmsg.Password
		default:
			return fmt.Errorf("unexpected message type: %#v", pmsg)
		}
	}

	pgdbName, err := getDBNameFromConnString(config.PostgresConnectionString)
	if err != nil {
		panic(err)
	}
	vdbName, err := getDBNameFromConnString(config.VerticaConnectionString)
	if err != nil {
		panic(err)
	}

	if vparams["database"] != pgdbName {
		return writeMessages(c, &pgproto3.ErrorResponse{
			Message: fmt.Sprintf("database %s does not exist", vparams["database"]),
			Code:    "3D000",
		})
	}
	vparams["database"] = vdbName

	vdb, vErr := connectToDB(RealDBOpener{}, "vertica", config.VerticaConnectionString, vparams)
	if vErr != nil {
		Logger.Error("Can't connect to Vertica")
		return writeMessages(
			c, &pgproto3.ErrorResponse{Message: vErr.Error()},
		)
	}
	c.vdb = vdb
	Logger.Info("established connection to Vertica")

	pgdb, pgErr := connectToDB(RealDBOpener{}, "postgres", config.PostgresConnectionString, nil)
	if pgErr != nil {
		Logger.Error("Can't connect to Postgres")
		return writeMessages(
			c, &pgproto3.ErrorResponse{Message: pgErr.Error()},
		)
	}
	c.pgdb = pgdb
	Logger.Info("established connection to Postgres")

	return writeMessages(c,
		&pgproto3.AuthenticationOk{},
		&pgproto3.ParameterStatus{Name: "server_version", Value: ServerVersion},
		&pgproto3.ParameterStatus{Name: "ApplicationName", Value: ApplicationName},
		&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
}

func handleSSLRequestMessage(ctx context.Context, c *Conn, msg *pgproto3.SSLRequest, config *ServerConfig) (*Conn, error) {
	Logger.Debug("received ssl request message", "message", msg)
	if config.TlsConfig == nil {
		return startWithoutSSL(ctx, c, config)
	} else {
		return startWithSSL(ctx, c, config)
	}
}

func startWithoutSSL(ctx context.Context, c *Conn, config *ServerConfig) (*Conn, error) {
	Logger.Info("SSL is not configured, use plain TCP")
	if _, err := c.Write([]byte("N")); err != nil {
		return c, err
	}
	return serveConnStartup(ctx, c, config)
}

func startWithSSL(ctx context.Context, c *Conn, config *ServerConfig) (*Conn, error) {
	Logger.Info("SSL is configured, use encrypted TLS")
	if _, err := c.Write([]byte("S")); err != nil {
		return c, err
	}

	tlsConn := tls.Server(c.Conn, config.TlsConfig)
	c = newConn(tlsConn)
	if err := tlsConn.Handshake(); err != nil {
		return c, err
	}
	Logger.Info("SSL connection established")
	return serveConnStartup(ctx, c, config)
}
