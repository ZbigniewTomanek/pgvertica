package pgvertica

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgtype"
)

func TestGetTypeOID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected uint32
	}{
		{"BOOL", "BOOL", pgtype.BoolOID},
		{"INT4", "INT4", pgtype.Int8OID},
		{"BYTEA", "BYTEA", pgtype.ByteaOID},
		{"Invalid type", "INVALID_TYPE", pgtype.TextOID},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := getTypeOID(test.input)
			if output != test.expected {
				t.Errorf("expected %d, got %d", test.expected, output)
			}
		})
	}
}

func TestGetCommandTag(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"INSERT", "INSERT INTO table VALUES (1, 'a')", "INSERT 0 1"},
		{"DELETE", "DELETE FROM table WHERE id = 1", "DELETE 1"},
		{"UPDATE", "UPDATE table SET name = 'b' WHERE id = 1", "UPDATE 1"},
		{"SELECT", "SELECT * FROM table", "SELECT 1"},
		{"Invalid command", "INVALID COMMAND", "INVALID"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := getCommandTag(test.input)
			if output != test.expected {
				t.Errorf("expected %s, got %s", test.expected, output)
			}
		})
	}
}

func TestToRowDescription(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "name", "email"}).
		AddRow(1, "John", "john@example.com").
		AddRow(2, "Doe", "doe@example.com")

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	res, _ := db.Query("SELECT")

	cols, _ := res.ColumnTypes()

	desc := toRowDescription(cols)

	if len(desc.Fields) != len(cols) {
		t.Fatalf("expected %v fields, got %v", len(cols), len(desc.Fields))
	}
}

func TestScanRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "name", "email"}).
		AddRow("1", "John", "john@example.com").
		AddRow("2", "Doe", "doe@example.com")

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	res, _ := db.Query("SELECT")

	cols, _ := res.ColumnTypes()

	for res.Next() {
		row, err := scanRowToText(res, cols)
		if err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if len(row.Values) != len(cols) {
			t.Fatalf("expected %v values, got %v", len(cols), len(row.Values))
		}

		for _, v := range row.Values {
			if v == nil {
				t.Fatalf("nil value in row")
			}
		}
	}
}
