package pgvertica

import (
	"database/sql"

	"github.com/jackc/pgproto3/v2"
)

type CursorType string

const (
	BINARY CursorType = "BINARY"
	TEXT   CursorType = "TEXT"
)

type Cursor struct {
	name        string
	query       string
	scanFunc    func(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error)
	rows        *sql.Rows
	columnTypes []*sql.ColumnType
}

func newCursor(name string, query string, ctype CursorType) *Cursor {
	var scanRowFunc func(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error)
	switch ctype {
	case BINARY:
		scanRowFunc = scanRowToBinary
	case TEXT:
		scanRowFunc = scanRowToText
	default:
		panic("Unknown cursor type")
	}

	return &Cursor{
		name:        name,
		query:       query,
		scanFunc:    scanRowFunc,
		rows:        nil,
		columnTypes: nil,
	}
}

func (c *Cursor) open(db *sql.DB) error {
	stmt, err := db.Prepare(c.query)
	if err != nil {
		Logger.Error("Error opening cursor", "name", c.name, "error", err)
		return err
	}
	result, err := stmt.Query()
	if err != nil {
		Logger.Error("Error opening cursor", "name", c.name, "error", err)
		return err
	}
	cTypes, err := result.ColumnTypes()
	if err != nil {
		Logger.Error("Error opening cursor", "name", c.name, "error", err)
		return err
	}

	c.rows = result
	c.columnTypes = cTypes
	Logger.Debug("Cursor opened", "name", c.name)
	return nil
}

func (c *Cursor) close() error {
	if c.rows == nil {
		Logger.Debug("Cursor already closed", "name", c.name)
		return nil
	}
	return c.rows.Close()
}

func (c *Cursor) fetch(n int) ([]pgproto3.Message, error) {
	Logger.Debug("Fetching rows using cursor", "name", c.name, "n", n)
	rows := make([]pgproto3.Message, 0, n)

	for range make([]int, n) {
		if !c.rows.Next() {
			break
		}
		row, error := c.scanFunc(c.rows, c.columnTypes)
		if error != nil {
			return nil, error
		}
		rows = append(rows, row)
	}
	messages := make([]pgproto3.Message, 0, len(rows)+1)
	messages = append(messages, toRowDescription(c.columnTypes))
	if len(rows) == 0 {
		return messages, nil
	}
	messages = append(messages, rows...)
	return messages, nil
}
