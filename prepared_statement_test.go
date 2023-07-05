package pgvertica

import (
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
)

func TestPreparedStatement_getBinds(t *testing.T) {
	ps := &PreparedStatement{
		name:          "test_name",
		query:         "test_query",
		parameterOIDs: []uint32{1, 2, 3},
		parameters:    nil,
		binds:         nil,
	}
	binds := ps.getBinds()
	assert.Equal(t, len(*binds), 3)
	assert.Equal(t, *binds, make([]interface{}, len(ps.parameterOIDs)))
}

func TestGetQuery(t *testing.T) {
	ps := &PreparedStatement{
		name:          "test_name",
		query:         "test_query",
		parameterOIDs: []uint32{},
		parameters:    nil,
		binds:         nil,
	}

	if query := ps.getQuery(); query != "test_query" {
		t.Errorf("Expected 'test_query' but got '%v'", query)
	}
}

func TestAddParameters_CorrectParameters(t *testing.T) {
	ps := &PreparedStatement{
		name:          "test_name",
		query:         "SELECT * FROM test WHERE a = ? AND b = ?",
		parameterOIDs: []uint32{1, 1},
		parameters:    nil,
		binds:         nil,
	}

	bindMsg := pgproto3.Bind{
		ParameterFormatCodes: []int16{0, 0},
		Parameters: [][]byte{
			[]byte("p1"),
			[]byte("p2"),
		},
	}

	if err := ps.addParameters(bindMsg); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assert.Equal(t, "SELECT * FROM test WHERE a = p1 AND b = p2", ps.getQuery())
}

func TestAddParameters_WrongParameters(t *testing.T) {
	ps := &PreparedStatement{
		name:          "test_name",
		query:         "SELECT * FROM test WHERE a = ? AND b = ?",
		parameterOIDs: []uint32{1, 1},
		parameters:    nil,
		binds:         nil,
	}

	bindMsg := pgproto3.Bind{
		ParameterFormatCodes: []int16{0},
		Parameters: [][]byte{
			[]byte("p1"),
		},
	}

	if err := ps.addParameters(bindMsg); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assert.Equal(t, "SELECT * FROM test WHERE a = null AND b = null", ps.getQuery())
}
