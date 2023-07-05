package pgvertica

import (
	"bytes"
	"crypto/tls"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"golang.org/x/exp/slog"
)

var Logger *slog.Logger = slog.Default()

func InitLogger(level slog.Level) {
	Logger = slog.New(slog.HandlerOptions{Level: level}.NewTextHandler(os.Stdout))
}

func getTypeOID(databaseType string) uint32 {
	switch databaseType {
	case "BOOL":
		return pgtype.BoolOID
	case "INT4", "INT8", "BIGINT", "INT32", "INT2", "INT":
		return pgtype.Int8OID
	case "OID", "XID", "INT2VECTOR", "OIDVECTOR", "_OID", "_INT2", "NAME", "_ACLITEM", "_TEXT", "REGPROC", "PG_NODE_TREE":
		return pgtype.TextOID
	case "FLOAT4", "FLOAT8", "DECIMAL", "NUMERIC", "FLOAT":
		return pgtype.Float8OID
	case "TIMESTAMP":
		return pgtype.TimestampOID
	case "DATE":
		return pgtype.DateOID
	case "TIME":
		return pgtype.TimeOID
	case "VARCHAR", "TEXT", "CHAR":
		return pgtype.TextOID
	case "BYTEA":
		return pgtype.ByteaOID
	default:
		return pgtype.TextOID
	}
}

func getCommandTag(query string) string {
	query = strings.ToLower(strings.TrimSpace(query))
	command := strings.Split(query, " ")[0]

	if command == "insert" {
		return "INSERT 0 1"
	} else if command == "delete" {
		return "DELETE 1"
	} else if command == "update" {
		return "UPDATE 1"
	} else if command == "select" {
		return "SELECT 1"
	} else {
		return strings.ToUpper(command)
	}
}

func toRowDescription(cols []*sql.ColumnType) *pgproto3.RowDescription {
	var desc pgproto3.RowDescription
	for _, col := range cols {
		desc.Fields = append(desc.Fields, pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          getTypeOID(col.DatabaseTypeName()),
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		})
	}
	return &desc
}

func scanRowToText(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error) {
	refs := make([]interface{}, len(cols))
	values := make([]interface{}, len(cols))
	for i := range refs {
		refs[i] = &values[i]
	}

	if err := rows.Scan(refs...); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	// Convert to appropriate values to return over Postgres wire protocol.
	row := pgproto3.DataRow{Values: make([][]byte, len(values))}
	for i := range values {
		if values[i] == nil {
			row.Values[i] = nil
		} else {
			dataType := cols[i].DatabaseTypeName()
			switch dataType {
			case "BOOL":
				row.Values[i] = []byte(fmt.Sprint(values[i].(bool)))
			case "INT4", "INT8", "INT32", "INT2":
				row.Values[i] = []byte(fmt.Sprint(values[i].(int64)))
			case "INT":
				row.Values[i] = []byte(fmt.Sprint(values[i].(int)))
			case "OID", "XID", "INT2VECTOR", "OIDVECTOR", "_OID", "_INT2":
				row.Values[i] = []byte(string(values[i].([]byte)))
			case "FLOAT4", "FLOAT8", "FLOAT", "NUMERIC":
				row.Values[i] = []byte(strconv.FormatFloat(values[i].(float64), 'f', -1, 64))
			case "TIMESTAMP", "TIMESTAMPTZ":
				row.Values[i] = []byte(values[i].(time.Time).Format("2006-01-02 15:04:05.999999"))
			case "DATE":
				row.Values[i] = []byte(values[i].(time.Time).Format("2006-01-02"))
			case "TIME":
				row.Values[i] = []byte(values[i].(time.Time).Format("15:04:05.999999"))
			case "VARCHAR", "TEXT", "CHAR":
				row.Values[i] = []byte(values[i].(string))
			case "NAME", "_ACLITEM", "_TEXT", "REGPROC", "PG_NODE_TREE":
				row.Values[i] = []byte(string(values[i].([]byte)))
			case "BYTEA":
				row.Values[i] = values[i].([]byte)
			default:
				Logger.Warn("This dataType is not explicitly supported", "dataType", dataType, "value", values[i])
				switch reflect.TypeOf(values[i]) {
				case reflect.TypeOf([]byte{}):
					row.Values[i] = []byte(values[i].([]byte))
				case reflect.TypeOf(""):
					row.Values[i] = []byte(values[i].(string))
				default:
					return nil, fmt.Errorf("unsupported data type: %s", dataType)

				}

			}
		}
	}
	return &row, nil
}

func scanRowToBinary(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error) {
	refs := make([]interface{}, len(cols))
	values := make([]interface{}, len(cols))
	for i := range refs {
		refs[i] = &values[i]
	}

	if err := rows.Scan(refs...); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	// Convert to appropriate values to return over Postgres wire protocol.
	row := pgproto3.DataRow{Values: make([][]byte, len(values))}
	for i := range values {
		if values[i] == nil {
			row.Values[i] = nil
		} else {
			dataType := cols[i].DatabaseTypeName()
			switch dataType {
			case "BOOL":
				if values[i].(bool) {
					row.Values[i] = []byte{1}
				} else {
					row.Values[i] = []byte{0}
				}
			case "INT4", "INT32":
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, int32(values[i].(int64)))
				row.Values[i] = buf.Bytes()
			case "INT8":
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, values[i].(int64))
				row.Values[i] = buf.Bytes()
			case "INT":
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, int64(values[i].(int)))
				row.Values[i] = buf.Bytes()
			case "FLOAT4":
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, float32(values[i].(float64)))
				row.Values[i] = buf.Bytes()
			case "FLOAT8", "FLOAT", "NUMERIC":
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, values[i].(float64))
				row.Values[i] = buf.Bytes()
			case "TIMESTAMP", "TIMESTAMPTZ":
				buf := new(bytes.Buffer)
				// PostgreSQL's timestamp starts from 2000-01-01
				binary.Write(buf, binary.BigEndian, values[i].(time.Time).Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Seconds())
				row.Values[i] = buf.Bytes()
			case "BYTEA", "VARCHAR", "TEXT", "CHAR":
				row.Values[i] = []byte(values[i].(string))
			default:
				return nil, fmt.Errorf("unsupported data type for binary format: %s", dataType)
			}
		}
	}
	return &row, nil
}

func LoadTLSConfig(x509SSLCertPath string) (*tls.Config, error) {
	if _, err := os.Stat(x509SSLCertPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("%s file does not exist", x509SSLCertPath)
	}

	Logger.Info("Create tls config from .pem file", "file", x509SSLCertPath)
	pemBytes, err := ioutil.ReadFile(x509SSLCertPath)
	if err != nil {
		return nil, err
	}

	keyPair, err := tls.X509KeyPair(pemBytes, pemBytes)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{keyPair},
		ClientAuth:   tls.NoClientCert,
	}
	return tlsConfig, nil
}
