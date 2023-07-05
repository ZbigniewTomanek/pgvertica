package pgvertica

import (
	"testing"
)

func mockedQueryUtil() *QueryUtil {
	return newQueryUtil("database")
}

func TestNormalizeQuery(t *testing.T) {
	qu := mockedQueryUtil()
	testCases := []struct {
		desc, input, expectedOutput string
	}{
		{
			desc:           "Test with regular string",
			input:          "   select * from users",
			expectedOutput: "SELECT * FROM USERS",
		},
		{
			desc:           "Test with comment line",
			input:          "   select * from users\n-- This is a comment",
			expectedOutput: "SELECT * FROM USERS\n-- THIS IS A COMMENT",
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			result := qu.normalizeQuery(tC.input)
			if result != tC.expectedOutput {
				t.Fatalf("expected %s, but got %s", tC.expectedOutput, result)
			}
		})
	}
}

func TestLimitQuery(t *testing.T) {
	qu := mockedQueryUtil()
	testCases := []struct {
		desc     string
		query    string
		limit    int
		expected string
	}{
		{
			desc:     "Test query without limit",
			query:    "SELECT * FROM table",
			limit:    10,
			expected: "SELECT * FROM table LIMIT 10",
		},
		{
			desc:     "Test query with limit",
			query:    "SELECT * FROM table LIMIT 5",
			limit:    10,
			expected: "SELECT * FROM table LIMIT 5",
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			result := qu.limitQuery(tC.query, tC.limit)
			if result != tC.expected {
				t.Fatalf("Expected %s, but got %s", tC.expected, result)
			}
		})
	}
}

func TestQueryReturnsNoRows(t *testing.T) {
	qu := mockedQueryUtil()
	testCases := []struct {
		desc, input    string
		expectedOutput bool
	}{
		{
			desc:           "Test with BEGIN query",
			input:          "   BEGIN TRANSACTION",
			expectedOutput: true,
		},
		{
			desc:           "Test with SELECT query",
			input:          "   SELECT * FROM users",
			expectedOutput: false,
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			result := qu.queryReturnsNoRows(tC.input)
			if result != tC.expectedOutput {
				t.Fatalf("Expected %v, but got %v", tC.expectedOutput, result)
			}
		})
	}
}

func TestRewriteQuery(t *testing.T) {
	qu := mockedQueryUtil()
	testCases := []struct {
		desc, input, expectedOutput string
	}{
		{
			desc:           "Test with integer type",
			input:          "SELECT id::integer FROM users",
			expectedOutput: "SELECT id::int FROM users",
		},
		{
			desc:           "Test with many types",
			input:          "SELECT id::integer, uuid::uuid FROM users",
			expectedOutput: "SELECT id::int, uuid::varchar(50) FROM users",
		},
		{
			desc:           "Test with no type to replace",
			input:          "SELECT id FROM users",
			expectedOutput: "SELECT id FROM users",
		},
		{
			desc:           "Test replace with AS",
			input:          "SELECT CAST(\"events\".\"City\" AS TEXT) AS \"City\" FROM \"project_notebooksrepo_sandbox\".\"events\" \"events\"\nLIMIT 100",
			expectedOutput: "SELECT CAST(\"events\".\"City\" AS varchar) AS \"City\" FROM \"project_notebooksrepo_sandbox\".\"events\" \"events\"\nLIMIT 100",
		},
		{
			desc:           "Test replacing DB name",
			input:          "SELECT * FROM database.schema.table",
			expectedOutput: "SELECT * FROM schema.table",
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			result := qu.rewriteQuery(tC.input)
			if result != tC.expectedOutput {
				t.Fatalf("Expected %s, but got %s", tC.expectedOutput, result)
			}
		})
	}
}

func TestIsDataQuery(t *testing.T) {
	qu := mockedQueryUtil()
	testCases := []struct {
		desc, input    string
		expectedOutput bool
	}{
		{
			desc:           "Test with technical table",
			input:          "SELECT * FROM PG_AGGREGATE",
			expectedOutput: false,
		},
		{
			desc:           "Test with non-technical table",
			input:          "SELECT * FROM my_schema.users",
			expectedOutput: true,
		},
		{
			desc:           "Test with non-technical table but wrong schema",
			input:          "SELECT * FROM my_schema_1.users",
			expectedOutput: true,
		},
		{
			desc:           "Test with SELECT not from table",
			input:          "SELECT 1",
			expectedOutput: false,
		},
	}
	schemaNames := []string{"my_schema"}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			result := qu.isDataQuery(tC.input, schemaNames)
			if result != tC.expectedOutput {
				t.Fatalf("Expected %v, but got %v", tC.expectedOutput, result)
			}
		})
	}
}
