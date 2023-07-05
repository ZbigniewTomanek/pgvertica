package pgvertica

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestContains(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		slice []string
		item  string
		want  bool
	}{
		{[]string{"apple", "banana", "cherry"}, "banana", true},
		{[]string{"apple", "banana", "cherry"}, "mango", false},
	}

	for _, tc := range testCases {
		got := contains(tc.slice, tc.item)
		assert.Equal(tc.want, got)
	}

}

func TestListTables(t *testing.T) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	rows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("table1").
		AddRow("table2").
		AddRow("table3")

	query := "SELECT table_name FROM tables"

	mock.ExpectQuery(query).WillReturnRows(rows)

	s := SchemasSynchronizator{
		vdb:  db,
		pgdb: db,
	}

	tables, err := s.listIds(db, query)
	assert.NoError(err)
	assert.Equal([]string{"table1", "table2", "table3"}, tables)
}

func TestDropTablePostgres(t *testing.T) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	schema := "public"
	table := "test_table"
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", schema, table)

	mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(1, 1))

	s := SchemasSynchronizator{
		vdb:  db,
		pgdb: db,
	}

	err = s.dropTablePostgres(schema, table)
	assert.NoError(err)
}

func TestDeleteNonExistentTablesInPG(t *testing.T) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	s := SchemasSynchronizator{
		vdb:  db,
		pgdb: db,
	}

	verticaRows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("table1").
		AddRow("table2")
	mock.ExpectQuery("SELECT table_name FROM v_catalog.tables WHERE table_schema='test_schema';").WillReturnRows(verticaRows)

	postgresRows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("table1").
		AddRow("table2").
		AddRow("table3")
	mock.ExpectQuery("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'test_schema';").WillReturnRows(postgresRows)

	mock.ExpectExec("DROP TABLE IF EXISTS test_schema.table3;").WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.deleteNonExistentTablesInPG("test_schema")
	assert.NoError(err)
}

func TestSyncDBschemas(t *testing.T) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	s := SchemasSynchronizator{
		vdb:  db,
		pgdb: db,
	}

	schemaRows := sqlmock.NewRows([]string{"schema_name"}).
		AddRow("schema1")

	tableRows := sqlmock.NewRows([]string{"table_name"}).
		AddRow("table1").
		AddRow("table2")

	mock.ExpectQuery("SELECT schema_name FROM v_catalog.schemata").WillReturnRows(schemaRows)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS schema1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT export_objects\('\', 'schema1'\)`).WillReturnRows(sqlmock.NewRows([]string{"exported_schema"}).AddRow(""))
	mock.ExpectQuery("SELECT table_name FROM v_catalog.tables WHERE table_schema='schema1';").WillReturnRows(tableRows)
	mock.ExpectQuery("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'schema1';").WillReturnRows(tableRows)

	err = s.SyncDBschemas()
	assert.NoError(err)
}

func TestRecreateSchemaOnPostgres(t *testing.T) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	s := SchemasSynchronizator{
		vdb:  db,
		pgdb: db,
	}

	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS test_schema").WillReturnResult(sqlmock.NewResult(1, 1))

	exportedSchema := "CREATE TABLE test_schema.table1 (...); CREATE TABLE test_schema.table2 (...);"
	mock.ExpectQuery("SELECT export_objects\\('\\', 'test_schema'\\)").WillReturnRows(sqlmock.NewRows([]string{"exported_schema"}).AddRow(exportedSchema))

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS test_schema.table1 (...)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS test_schema.table2 (...)").WillReturnResult(sqlmock.NewResult(1, 1))
	err = s.recreateSchemaOnPostgres("test_schema")
	assert.NoError(err)
}
