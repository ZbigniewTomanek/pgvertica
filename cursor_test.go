package pgvertica

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestOpenCursor(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	query := "SELECT * FROM test"

	mock.ExpectPrepare("SELECT")
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"col1"}).AddRow("val1"))

	cursor := newCursor("test", query, TEXT)
	err := cursor.open(db)

	assert.NoError(t, err)
	assert.NotNil(t, cursor.rows)
	assert.NotNil(t, cursor.columnTypes)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestOpenCursorError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	mock.ExpectPrepare("SELECT").WillReturnError(errors.New("prepare error"))

	cursor := newCursor("test", "SELECT * FROM test", TEXT)
	err := cursor.open(db)

	assert.Error(t, err)
	assert.Nil(t, cursor.rows)
	assert.Nil(t, cursor.columnTypes)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCloseCursor(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	rows := sqlmock.NewRows([]string{"col1"}).AddRow("val1")
	mock.ExpectPrepare("SELECT")
	mock.ExpectQuery("SELECT").WillReturnRows(rows).RowsWillBeClosed()

	cursor := newCursor("test", "SELECT * FROM test", TEXT)
	_ = cursor.open(db)
	err := cursor.close()

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFetchCursor(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	rows := sqlmock.NewRows([]string{"col1"}).AddRow("val1")
	mock.ExpectPrepare("SELECT")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	cursor := newCursor("test", "SELECT * FROM test", TEXT)
	_ = cursor.open(db)
	messages, err := cursor.fetch(1)

	assert.NoError(t, err)
	assert.Len(t, messages, 2) // 1 row description + 1 data row
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFetchCursorError(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()

	rows := sqlmock.NewRows([]string{"col1"}).AddRow("val1")
	mock.ExpectPrepare("SELECT")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	cursor := newCursor("test", "SELECT * FROM test", BINARY) // Binary will cause an error
	_ = cursor.open(db)
	messages, err := cursor.fetch(1)

	assert.Error(t, err)
	assert.Nil(t, messages)
	assert.NoError(t, mock.ExpectationsWereMet())
}
