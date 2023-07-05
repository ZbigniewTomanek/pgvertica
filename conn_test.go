package pgvertica

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestBuildConnectionString(t *testing.T) {
	testCases := []struct {
		desc   string
		scheme string
		params map[string]string
		host   string
		port   int
		want   string
	}{
		{
			desc:   "standard connection",
			scheme: "postgres",
			params: map[string]string{
				"user":     "test",
				"password": "test",
				"database": "testdb",
			},
			host: "localhost",
			port: 5432,
			want: "postgres://test:test@localhost:5432/testdb",
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			got := buildConnectionString(tC.scheme, tC.params, tC.host, tC.port)
			if got != tC.want {
				t.Errorf("BuildConnectionString(%s, %v, %s, %d) = %s; want %s", tC.scheme, tC.params, tC.host, tC.port, got, tC.want)
			}
		})
	}
}

type MockDBOpener struct {
	db  *sql.DB
	err error
}

func (mo MockDBOpener) Open(driverName, dataSourceName string) (*sql.DB, error) {
	return mo.db, mo.err
}

func TestConnectToDB(t *testing.T) {
	db, mock, err := sqlmock.NewWithDSN("", sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("An error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	testCases := []struct {
		opener           DBOpener
		desc             string
		driverName       string
		connectionString string
		params           map[string]string
		mockFunc         func()
		expectedError    error
	}{
		{
			opener: MockDBOpener{
				db:  db,
				err: nil,
			},
			desc:             "successful connection",
			driverName:       "postgres",
			connectionString: "postgres://test:test@localhost:5432/testdb",
			params: map[string]string{
				"user":     "test",
				"password": "test",
				"database": "testdb",
			},
			mockFunc: func() {
				mock.ExpectPing()
			},
			expectedError: nil,
		},
		{
			opener: MockDBOpener{
				db:  db,
				err: nil,
			},
			desc:             "failed connection",
			driverName:       "postgres",
			connectionString: "postgres://test:test@localhost:5432/testdb",
			params: map[string]string{
				"user":     "test",
				"password": "test",
				"database": "testdb",
			},
			mockFunc: func() {
				mock.ExpectPing().WillReturnError(sql.ErrConnDone)
			},
			expectedError: sql.ErrConnDone,
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			tC.mockFunc()
			_, err := connectToDB(tC.opener, tC.driverName, tC.connectionString, tC.params)
			assert.Equal(t, tC.expectedError, err)
			err = mock.ExpectationsWereMet()
			assert.NoError(t, err)
		})
	}
}
