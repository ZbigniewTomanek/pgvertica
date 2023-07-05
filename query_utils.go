package pgvertica

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	POSTGRES_TO_VERTICA_TYPE_MAPPING = map[string]string{
		"boolean":     "boolean",
		"smallint":    "int",
		"integer":     "int",
		"bigint":      "int",
		"real":        "float",
		"numeric":     "numeric",
		"decimal":     "numeric",
		"date":        "date",
		"timestamp":   "timestamp",
		"timestamptz": "timestamptz",
		"time":        "time",
		"interval":    "interval",
		"varchar":     "varchar",
		"text":        "varchar",
		"bytea":       "varbinary",
		"json":        "long varchar",
		"jsonb":       "long varchar",
		"uuid":        "varchar(50)",
		"inet":        "varchar(39)",
		"cidr":        "varchar(43)",
	}
	VERTICA_TO_POSTGRES_TYPE_MAPPING = map[string]string{
		"boolean":     "boolean",
		"int":         "bigint",
		"float":       "double precision",
		"numeric":     "numeric",
		"date":        "date",
		"timestamp":   "timestamp",
		"timestamptz": "timestamptz",
		"time":        "time",
		"timetz":      "timetz",
		"interval":    "interval",
	}

	postgresTechnicalTables = [...]string{
		"PG_AGGREGATE",
		"PG_AM",
		"PG_AMOP",
		"PG_AMPROC",
		"PG_ATTRDEF",
		"PG_ATTRIBUTE",
		"PG_AUTH_MEMBERS",
		"PG_AUTHID",
		"PG_CAST",
		"PG_CLASS",
		"PG_COLLATION",
		"PG_CONSTRAINT",
		"PG_CONVERSION",
		"PG_DATABASE",
		"PG_ENUM",
		"PG_FOREIGN_DATA_WRAPPER",
		"PG_FOREIGN_SERVER",
		"PG_FOREIGN_TABLE",
		"PG_INDEX",
		"PG_INHERITS",
		"PG_LANGUAGE",
		"PG_LARGEOBJECT",
		"PG_LARGEOBJECT_METADATA",
		"PG_NAMESPACE",
		"PG_OPCLASS",
		"PG_OPERATOR",
		"PG_OPFAMILY",
		"PG_PLTEMPLATE",
		"PG_PROC",
		"PG_REWRITE",
		"PG_SHDEPEND",
		"PG_SHDESCRIPTION",
		"PG_STATISTIC",
		"PG_TABLESPACE",
		"PG_TRIGGER",
		"PG_TS_CONFIG",
		"PG_TS_CONFIG_MAP",
		"PG_TS_DICT",
		"PG_TS_PARSER",
		"PG_TS_TEMPLATE",
		"PG_TYPE",
		"PG_USER_MAPPING",
		"PG_DIST_NODE",
		"INFORMATION_SCHEMA",
	}
)

type DeclareCursorQuery struct {
	name       string
	query      string
	cursorType string
}

type FetchQuery struct {
	Direction  string
	Count      int
	CursorName string
}

type QueryUtil struct {
	fromDBRegexp *regexp.Regexp
}

func newQueryUtil(pgDBName string) *QueryUtil {
	dbRegexp := regexp.MustCompile(fmt.Sprintf(`(?i)FROM\s+(["']?%s["']?\.)`, regexp.QuoteMeta(pgDBName)))
	return &QueryUtil{fromDBRegexp: dbRegexp}
}

func (q *QueryUtil) normalizeQuery(query string) string {
	normalizedQuery := strings.TrimSpace(strings.ToUpper(query))
	lines := strings.Split(normalizedQuery, "\n")
	var filteredLines []string
	for _, line := range lines {
		if !strings.HasPrefix("--", strings.TrimSpace(line)) {
			filteredLines = append(filteredLines, line)
		}
	}
	return strings.Join(filteredLines, "\n")
}

func (q *QueryUtil) limitQuery(query string, limit int) string {
	normalizedQuery := q.normalizeQuery(query)
	if limitRegexp.MatchString(normalizedQuery) {
		return query
	} else {
		query := strings.Trim(strings.TrimSpace(query), ";")
		return fmt.Sprintf("%s LIMIT %d", query, limit)
	}
}

func (q *QueryUtil) isDeallocateQuery(query string) bool {
	normalizedQuery := q.normalizeQuery(query)
	return strings.HasPrefix(normalizedQuery, "DEALLOCATE")
}

func (q *QueryUtil) isCloseQuery(query string) bool {
	return closeQueryRegexp.MatchString(query)
}

func (q *QueryUtil) parseCloseQuery(query string) (string, error) {
	matches := closeQueryRegexp.FindStringSubmatch(query)
	if len(matches) != 2 {
		return "", fmt.Errorf("unable to parse close query: %s", query)
	}
	return matches[1], nil
}

func (q *QueryUtil) queryReturnsNoRows(query string) bool {
	var nowRows = false
	normalizedQuery := q.normalizeQuery(query)
	if strings.HasPrefix(normalizedQuery, "BEGIN") {
		nowRows = true
	}
	if strings.HasPrefix(normalizedQuery, "SET") {
		nowRows = true
	}
	if strings.HasPrefix(normalizedQuery, "COMMIT") {
		nowRows = true
	}
	if strings.HasPrefix(normalizedQuery, "ROLLBACK") {
		nowRows = true
	}
	if strings.HasPrefix(normalizedQuery, "DISCARD") {
		nowRows = true
	}
	return nowRows
}

func (q *QueryUtil) isBeginQuery(query string) bool {
	normalizedQuery := q.normalizeQuery(query)
	return strings.HasPrefix(normalizedQuery, "BEGIN")
}

func (q *QueryUtil) queryDiscardsTransaction(query string) bool {
	normalizedQuery := q.normalizeQuery(query)
	return strings.HasPrefix(normalizedQuery, "COMMIT") || strings.HasPrefix(normalizedQuery, "ROLLBACK") || strings.HasPrefix(normalizedQuery, "DISCARD")
}

func (q *QueryUtil) queryShouldReturnEmptyResponse(query string) bool {
	var emptyResponse = false
	normalizedQuery := q.normalizeQuery(query)
	if strings.HasPrefix(normalizedQuery, "BEGIN") {
		emptyResponse = true
	}
	if query == "" {
		emptyResponse = true
	}
	return emptyResponse

}

func (q *QueryUtil) isDataQuery(query string, synchronizedSchemas []string) bool {
	// data query is a query which selects rows from the Vertica data table and should be routed to Vertica
	normalizedQuery := strings.TrimSpace(strings.ToUpper(query))
	for _, tableName := range postgresTechnicalTables {
		if strings.Contains(normalizedQuery, tableName) {
			return false
		}
	}
	if strings.HasPrefix(normalizedQuery, "SELECT") {
		var containsSynchronizedSchema = false
		for _, schema := range synchronizedSchemas {
			schemaUpper := strings.ToUpper(schema)
			if strings.Contains(normalizedQuery, schemaUpper) {
				containsSynchronizedSchema = true
			}
		}
		return containsSynchronizedSchema && strings.Contains(normalizedQuery, "FROM")
	}
	return false

}

func (q *QueryUtil) mapPostgresToVerticaType(dataType string) string {
	dataType = strings.ToLower(dataType)

	if verticaType, ok := POSTGRES_TO_VERTICA_TYPE_MAPPING[dataType]; ok {
		return verticaType
	}
	return dataType
}

func (q *QueryUtil) replacePostgresDataTypes(query string) string {
	replaced := typeRegex.ReplaceAllStringFunc(query, func(match string) string {
		parts := typeRegex.FindStringSubmatch(match)
		if len(parts) == 2 {
			verticaType := q.mapPostgresToVerticaType(strings.TrimSpace(parts[1]))
			if verticaType == "" {
				return match
			}
			return "::" + verticaType
		}
		return match
	})

	replaced = typeCastRegex.ReplaceAllStringFunc(replaced, func(match string) string {
		parts := typeCastRegex.FindStringSubmatch(match)
		if len(parts) == 3 {
			verticaType := q.mapPostgresToVerticaType(strings.TrimSpace(parts[1]))
			if verticaType == "" {
				return match
			}
			return "AS " + verticaType + ")"
		}
		return match
	})

	return replaced
}

func (q *QueryUtil) rewriteQuery(query string) string {
	query = q.replacePostgresDataTypes(query)
	query = q.fromDBRegexp.ReplaceAllString(query, "FROM ")
	return query
}

func parseSetQuery(expression string) (string, string, error) {
	expression = strings.TrimSpace(expression)

	if !strings.HasPrefix(expression, "SET ") {
		return "", "", fmt.Errorf("expression must start with 'SET '")
	}

	expression = strings.TrimPrefix(expression, "SET ")

	var parts []string = nil
	for _, separator := range []string{"TO", "="} {
		split := strings.Split(expression, separator)

		if len(split) == 2 {
			parts = split
			break
		}
	}

	if parts == nil {
		return "", "", fmt.Errorf("can't parse expression, must contain 'TO' or '='")
	}

	paramName := strings.TrimSpace(parts[0])
	paramValue := strings.Trim(parts[1], " '\"")

	return paramName, paramValue, nil
}

func (q *QueryUtil) isSetQuery(query string) bool {
	normalizedQuery := q.normalizeQuery(query)
	return strings.HasPrefix(normalizedQuery, "SET")
}

func (q *QueryUtil) isDeclareCursorQuery(query string) bool {
	return declareCursorRegexp.MatchString(query)
}

func (q *QueryUtil) parseDeclareCursorQuery(query string) (*DeclareCursorQuery, error) {
	matches := declareCursorRegexp.FindStringSubmatch(query)
	if len(matches) != 4 {
		return nil, fmt.Errorf("can't parse declare cursor query")
	}
	return &DeclareCursorQuery{
		name:       matches[1],
		query:      matches[3],
		cursorType: matches[2],
	}, nil
}

func (q *QueryUtil) isFetchQuery(query string) bool {
	return fetchQueryRegexp.MatchString(query)
}

func (q *QueryUtil) parseFetchQuery(query string) (*FetchQuery, error) {
	match := fetchQueryRegexp.FindStringSubmatch(query)

	paramsMap := make(map[string]string)
	for i, name := range fetchQueryRegexp.SubexpNames() {
		if i > 0 && i <= len(match) {
			paramsMap[name] = match[i]
		}
	}

	count := 0
	if paramsMap["Count"] != "" {
		var err error
		count, err = strconv.Atoi(paramsMap["Count"])
		if err != nil {
			return nil, err
		}
	}

	return &FetchQuery{
		Direction:  strings.ToUpper(paramsMap["Direction"]),
		Count:      count,
		CursorName: paramsMap["CursorName"],
	}, nil
}

var (
	typeRegex           = regexp.MustCompile(`(?i)::([a-zA-Z0-9]+)`)
	typeCastRegex       = regexp.MustCompile(`(?i)AS (([a-zA-Z0-9]+))\)`)
	limitRegexp         = regexp.MustCompile(`LIMIT (0|[1-9][0-9]*)`)
	declareCursorRegexp = regexp.MustCompile(`(?i)DECLARE\s+(\w+)\s+(.*) CURSOR .*? FOR\s+(.*)`)
	fetchQueryRegexp    = regexp.MustCompile(`(?i)FETCH\s+(?P<Direction>\w+)?\s*(?P<Count>\d+)?\s*(FROM|IN)?\s*(?P<CursorName>\w+)?`)
	closeQueryRegexp    = regexp.MustCompile(`(?i)CLOSE\s+(.*)`)
)
