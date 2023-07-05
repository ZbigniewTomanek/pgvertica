package pgvertica

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

type SchemasSynchronizator struct {
	vdb  *sql.DB
	pgdb *sql.DB
}

func NewSchemasSynchronizator(vConnStr string, pgConnStr string) (*SchemasSynchronizator, error) {
	vdb, vErr := connectToDB(RealDBOpener{}, "vertica", vConnStr, nil)
	if vErr != nil {
		Logger.Error("Can't connect to Vertica")
		return nil, vErr
	}
	Logger.Info("established connection to Vertica")

	pgdb, pgErr := connectToDB(RealDBOpener{}, "postgres", pgConnStr, nil)
	if pgErr != nil {
		Logger.Error("Can't connect to Postgres")
		return nil, vErr
	}
	Logger.Info("established connection to Postgres")

	return &SchemasSynchronizator{
		vdb:  vdb,
		pgdb: pgdb,
	}, nil
}

func (s *SchemasSynchronizator) Close() (err error) {
	if s.vdb != nil {
		if e := s.vdb.Close(); err == nil {
			err = e
		}
	}

	if s.pgdb != nil {
		if e := s.pgdb.Close(); err == nil {
			err = e
		}
	}

	return err
}

func (s *SchemasSynchronizator) SyncDBschemas() error {
	var err error
	var schemasToSync []string
	if schemasToSync, err = s.ListSchemas(); err != nil {
		return err
	}
	if len(schemasToSync) == 0 {
		Logger.Info("No schemas to sync")
		return nil
	}
	Logger.Info("Sync schemas", "schemas", schemasToSync)
	for _, schemaName := range schemasToSync {
		if err = s.recreateSchemaOnPostgres(schemaName); err != nil {
			return err
		}

		if err := s.deleteNonExistentTablesInPG(schemaName); err != nil {
			return err
		}

	}

	if err = s.syncRoles(schemasToSync); err != nil {
		Logger.Error("Error syncing roles", "error", err)
	}
	Logger.Info("Schemas synced")

	return nil
}

func (s *SchemasSynchronizator) syncRoles(schemas []string) error {
	Logger.Debug("Sync roles")
	roles, err := s.listRoles()
	if err != nil {
		return err
	}

	for _, role := range roles {
		if exists, err := s.roleExists(role); err == nil {
			if !exists {
				create_role_sql := fmt.Sprintf("CREATE ROLE %s", role)
				if _, err := s.pgdb.Exec(create_role_sql); err != nil {
					return err
				}
				Logger.Debug("Role created", "role", role)
			} else {
				Logger.Debug("Role already exists", "role", role)
			}
		} else {
			return err
		}
	}

	for _, schema := range schemas {
		for _, role := range roles {
			if err := s.grantPermissionToSchema(schema, role); err != nil {
				return err
			}
			if err := s.grantPermissionToTablesInSchema(schema, role); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *SchemasSynchronizator) grantPermissionToSchema(schemaName string, role string) error {
	grant_sql := fmt.Sprintf("GRANT ALL ON SCHEMA %s TO %s;", schemaName, role)
	if _, err := s.pgdb.Exec(grant_sql); err != nil {
		Logger.Error("Error on query", "query", grant_sql, "error", err)
		return err
	}
	return nil
}

func (s *SchemasSynchronizator) grantPermissionToTablesInSchema(schemaName string, role string) error {
	tables, err := s.listTablesInSchemaPostgres(schemaName)
	if err != nil {
		return err
	}

	for _, table := range tables {
		sql := fmt.Sprintf("GRANT ALL ON TABLE %s.%s TO %s;", schemaName, table, role)
		if _, err := s.pgdb.Exec(sql); err != nil {
			Logger.Error("Error on query", "query", sql, "error", err)
			return err
		}
	}
	return nil
}

func (s *SchemasSynchronizator) listRoles() ([]string, error) {
	rolesSql := "SELECT name from v_catalog.roles;"
	roles, err := s.listIds(s.vdb, rolesSql)
	if err != nil {
		return nil, err
	}
	usersSql := "SELECT user_name from v_catalog.users;"
	users, err := s.listIds(s.vdb, usersSql)
	if err != nil {
		return nil, err
	}
	return append(roles, users...), nil
}

func (s *SchemasSynchronizator) roleExists(role string) (bool, error) {
	var exists bool

	query := fmt.Sprintf("SELECT 1 FROM pg_roles WHERE rolname='%s'", role)
	err := s.pgdb.QueryRow(query).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (s *SchemasSynchronizator) deleteNonExistentTablesInPG(schema string) error {
	verticaTables, err := s.listTablesInSchemaVertica(schema)
	if err != nil {
		return err
	}

	postgresTables, err := s.listTablesInSchemaPostgres(schema)
	if err != nil {
		return err
	}

	for _, tableName := range postgresTables {
		if !contains(verticaTables, tableName) {
			Logger.Debug("Drop table", "table", tableName)
			if err := s.dropTablePostgres(schema, tableName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *SchemasSynchronizator) listTablesInSchemaVertica(schema string) ([]string, error) {
	query := fmt.Sprintf(`SELECT table_name FROM v_catalog.tables WHERE table_schema='%s';`, schema)
	return s.listIds(s.vdb, query)
}

func (s *SchemasSynchronizator) listTablesInSchemaPostgres(schema string) ([]string, error) {
	query := fmt.Sprintf(`SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '%s';`, schema)
	return s.listIds(s.pgdb, query)
}

func (s *SchemasSynchronizator) listIds(db *sql.DB, query string) ([]string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		ids = append(ids, tableName)
	}

	return ids, rows.Err()
}

func (s *SchemasSynchronizator) dropTablePostgres(schema, table string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", schema, table)
	_, err := s.pgdb.Exec(query)
	return err
}

func contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}

func (s *SchemasSynchronizator) recreateSchemaOnPostgres(schemaName string) error {
	_, err := s.pgdb.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schemaName))
	if err != nil {
		return err
	}

	exportedSchema, err := s.exportSchema(schemaName)
	if err != nil {
		return err
	}

	for _, createTableQuery := range getCreateTableStatements(exportedSchema) {
		translatedQuery := translateCreateTableToPostgres(createTableQuery)
		Logger.Debug("Run translated query on postgres", "query", translatedQuery)
		if _, err := s.pgdb.Exec(translatedQuery); err != nil {
			Logger.Error("Error on query", "query", translatedQuery, "error", err)
		}

	}
	return nil

}

func (s *SchemasSynchronizator) ListSchemas() ([]string, error) {
	rows, err := s.vdb.Query("SELECT schema_name FROM v_catalog.schemata")
	if err != nil {
		return nil, fmt.Errorf("failed to list schemas: %v", err)
	}
	defer rows.Close()

	var schemas []string = make([]string, 0)
	for rows.Next() {
		var schemaName string
		err := rows.Scan(&schemaName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan schema_name: %v", err)
		}
		if !strings.HasPrefix(schemaName, "v_") && schemaName != "public" {
			schemas = append(schemas, schemaName)

		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate through rows: %v", err)
	}

	return schemas, nil
}

func (s *SchemasSynchronizator) exportSchema(schemaName string) (string, error) {
	row := s.vdb.QueryRow(fmt.Sprintf("SELECT export_objects('', '%s')", schemaName))

	var exportedSchema string
	err := row.Scan(&exportedSchema)
	if err != nil {
		return "", fmt.Errorf("failed to export schema '%s': %v", schemaName, err)
	}

	return exportedSchema, nil
}

func getCreateTableStatements(schema string) []string {
	lines := strings.Split(schema, "\n\n")

	var filteredLines []string
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		if createTableRegex.MatchString(trimmedLine) {
			filteredLines = append(filteredLines, line)
		}
	}

	return filteredLines
}

func translateCreateTableToPostgres(createTableStatement string) string {
	createTableStatement = strings.Replace(createTableStatement, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 15)
	for verticaType, postgresType := range VERTICA_TO_POSTGRES_TYPE_MAPPING {
		createTableStatement = strings.ReplaceAll(createTableStatement, verticaType, postgresType)
	}
	createTableStatement = replaceTextRegex.ReplaceAllString(createTableStatement, "text")
	createTableStatement = partitionByRegex.ReplaceAllString(createTableStatement, "")

	return createTableStatement
}

var (
	createTableRegex = regexp.MustCompile(`(?i)^CREATE\s+TABLE`)
	replaceTextRegex = regexp.MustCompile(`text\(\d*?\)`)
	partitionByRegex = regexp.MustCompile(`PARTITION BY \(.*\)`)
)
