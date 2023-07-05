package pgvertica

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jackc/pgproto3/v2"
	"github.com/lib/pq"
	vertigo "github.com/vertica/vertica-sql-go"
)

type QueryExecutor struct {
	ctx                   context.Context
	mb                    MessageBufferInterface
	queryUtil             *QueryUtil
	conn                  *Conn
	preparedStatements    map[string]*PreparedStatement
	cursors               map[string]*Cursor
	inTransaction         bool
	maxBufferSize         int
	currPreparedStatement *PreparedStatement
	synchronizedSchemas   []string
}

func newQueryExecutor(ctx context.Context, conn *Conn, config *ServerConfig) *QueryExecutor {
	pgdb_name, err := getDBNameFromConnString(config.PostgresConnectionString)
	if err != nil {
		panic(err)
	}
	return &QueryExecutor{
		ctx:                   ctx,
		mb:                    newMessagesBuffer(conn.Conn),
		conn:                  conn,
		queryUtil:             newQueryUtil(pgdb_name),
		preparedStatements:    make(map[string]*PreparedStatement),
		cursors:               make(map[string]*Cursor),
		inTransaction:         false,
		maxBufferSize:         16,
		currPreparedStatement: nil,
		synchronizedSchemas:   config.SynchronizedSchemas,
	}
}

func (qe *QueryExecutor) getTransactionStatus() byte {
	if qe.inTransaction {
		return 'T'
	} else {
		return 'I'
	}
}

func (qe *QueryExecutor) getErrorResponse(err error) *pgproto3.ErrorResponse {
	if pqerr, ok := err.(*pq.Error); ok {
		return &pgproto3.ErrorResponse{
			Severity:       pqerr.Severity,
			Code:           string(pqerr.Code),
			Message:        pqerr.Message,
			Detail:         pqerr.Detail,
			Hint:           pqerr.Hint,
			InternalQuery:  pqerr.InternalQuery,
			Where:          pqerr.Where,
			SchemaName:     pqerr.Schema,
			TableName:      pqerr.Table,
			ColumnName:     pqerr.Column,
			DataTypeName:   pqerr.DataTypeName,
			ConstraintName: pqerr.Constraint,
			File:           pqerr.File,
			Routine:        pqerr.Routine,
		}
	} else if verr, ok := err.(*vertigo.VError); ok {
		return &pgproto3.ErrorResponse{
			Severity: verr.Severity,
			Code:     verr.ErrorCode,
			Message:  verr.Message,
			Detail:   verr.Detail,
			Hint:     verr.Hint,
		}
	} else {
		return &pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "0A000",
			Message:  err.Error(),
		}
	}

}

func (qe *QueryExecutor) handleQueryMessage(msg *pgproto3.Query) (err error) {
	Logger.Info("received query", "query", msg.String)
	query := msg.String
	commandTag := getCommandTag(query)

	if qe.queryUtil.isDeallocateQuery(msg.String) {
		qe.mb.queueMessages(
			&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
		return qe.mb.sendQueuedMessages()
	}

	if qe.queryUtil.isCloseQuery(query) {
		qe.closeCursor(query)
		qe.mb.queueMessages(
			&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
		return qe.mb.sendQueuedMessages()
	}

	if qe.queryUtil.isDeclareCursorQuery(query) {
		qe.declareCursor(query)
		return qe.mb.sendQueuedMessages()
	}

	if qe.queryUtil.isFetchQuery(query) {
		qe.fetchFromCursor(query)
		return qe.mb.sendQueuedMessages()
	}

	rows, dberr := qe.executeQuery(query)

	if dberr != nil {
		Logger.Error("Query error", "query", query, "error", dberr)

		qe.mb.queueMessages(
			qe.getErrorResponse(dberr),
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
		if err := qe.mb.sendQueuedMessages(); err != nil {
			return err
		}
		return dberr
	}

	defer rows.Close()
	if qe.queryUtil.isBeginQuery(query) {
		qe.inTransaction = true
	}

	if qe.queryUtil.queryDiscardsTransaction(query) {
		qe.inTransaction = false
	}

	if qe.queryUtil.queryReturnsNoRows(query) {
		qe.mb.queueMessages(
			&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
		return qe.mb.sendQueuedMessages()
	}

	if qe.queryUtil.queryShouldReturnEmptyResponse(query) {
		qe.mb.queueMessages(
			&pgproto3.EmptyQueryResponse{},
			&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
		return qe.mb.sendQueuedMessages()
	}

	cols, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("column types: %w", err)
	}

	qe.mb.queueMessages(toRowDescription(cols))

	if err := qe.writeRowsInChunks(rows, cols); err != nil {
		return err
	}

	qe.mb.queueMessages(
		&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
		&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
	)
	return qe.mb.sendQueuedMessages()
}

func (qe *QueryExecutor) declareCursor(declareQuery string) {
	commandTag := getCommandTag(declareQuery)
	parsedQuery, err := qe.queryUtil.parseDeclareCursorQuery(declareQuery)
	query := qe.queryUtil.rewriteQuery(parsedQuery.query)
	if err != nil {
		qe.mb.queueMessages(
			qe.getErrorResponse(err),
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
	}

	cursor := newCursor(parsedQuery.name, query, CursorType(parsedQuery.cursorType))
	err = cursor.open(qe.conn.vdb)
	if err != nil {
		qe.mb.queueMessages(
			qe.getErrorResponse(err),
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
	}

	qe.cursors[cursor.name] = cursor
	Logger.Info("declare and open cursor", "cursor", cursor.name, "query", query)
	qe.mb.queueMessages(
		&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
		&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
	)
}

func (qe *QueryExecutor) fetchFromCursor(fetchQueryStr string) {
	commandTag := getCommandTag(fetchQueryStr)
	fetchQuery, err := qe.queryUtil.parseFetchQuery(fetchQueryStr)
	if err != nil {
		qe.mb.queueMessages(
			qe.getErrorResponse(err),
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
	}
	cursor, ok := qe.cursors[fetchQuery.CursorName]
	if !ok {
		qe.mb.queueMessages(
			qe.getErrorResponse(fmt.Errorf("cursor %s does not exist", fetchQuery.CursorName)),
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
	}
	messages, err := cursor.fetch(fetchQuery.Count)
	if err != nil {
		qe.mb.queueMessages(
			qe.getErrorResponse(err),
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
	}
	qe.mb.queueMessages(
		messages...,
	)
	qe.mb.queueMessages(
		&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
		&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
	)
}

func (qe *QueryExecutor) closeCursor(closeQuery string) {
	cursorName, err := qe.queryUtil.parseCloseQuery(closeQuery)
	if err != nil {
		Logger.Error("close cursor error", "error", err)
		return
	}
	cursor, ok := qe.cursors[cursorName]
	if !ok {
		Logger.Error("cursor does not exist", "cursor", cursorName)
		return
	}
	cursor.close()
}

func (qe *QueryExecutor) handleBindMessage(bmsg *pgproto3.Bind) error {
	stmt, ok := qe.preparedStatements[bmsg.PreparedStatement]
	if !ok {
		qe.mb.queueMessages(
			&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "26000",
				Message:  "prepared statement \"" + bmsg.PreparedStatement + "\" does not exist",
			},
		)
		return fmt.Errorf("prepared statement \"%s\" does not exist", bmsg.PreparedStatement)
	}

	if err := stmt.addParameters(*bmsg); err != nil {
		qe.mb.queueMessages(
			&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "22023",
				Message:  "invalid parameter value",
			},
		)
		return fmt.Errorf("add parameters: %w", err)
	}

	qe.preparedStatements[bmsg.PreparedStatement] = stmt
	qe.mb.queueMessages(&pgproto3.BindComplete{})
	return qe.handlePreparedStatement(stmt)
}

func (qe *QueryExecutor) handleParseMessage(pmsg *pgproto3.Parse) error {
	query := pmsg.Query
	preparedStatement := PreparedStatement{
		name:          pmsg.Name,
		query:         query,
		parameterOIDs: pmsg.ParameterOIDs,
	}
	// cache named prepared statements
	if preparedStatement.name != "" {
		qe.preparedStatements[preparedStatement.name] = &preparedStatement
	}

	qe.mb.queueMessages(&pgproto3.ParseComplete{})
	return qe.handlePreparedStatement(&preparedStatement)
}

func (qe *QueryExecutor) handlePreparedStatement(preparedStatement *PreparedStatement) error {
	qe.currPreparedStatement = preparedStatement
	for {
		msg, err := qe.conn.receiver.Receive()
		if err != nil {
			return fmt.Errorf("receive message during parse: %w", err)
		}
		Logger.Debug("[recv][qe]", "type", reflect.TypeOf(msg), "msg", msg)

		switch msg := msg.(type) {
		case *pgproto3.Bind:
			if err := qe.handleBind(msg, preparedStatement); err != nil {
				Logger.Error("Error handling bind", "error", err)
			}
		case *pgproto3.Describe:
			if err := qe.handleDescribe(msg); err != nil {
				Logger.Error("Error handling describe", "error", err)
			}

		case *pgproto3.Execute:
			qe.currPreparedStatement = nil
			return qe.handleExecute(preparedStatement)

		case *pgproto3.Sync:
			qe.currPreparedStatement = nil
			qe.mb.queueMessages(&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()})
			return qe.mb.sendQueuedMessages()

		case *pgproto3.Close:
			qe.currPreparedStatement = nil
			qe.mb.queueMessages(&pgproto3.CloseComplete{})
			return nil

		default:
			return fmt.Errorf("unexpected message type during parse: %#v", msg)
		}
	}
}

func (qe *QueryExecutor) handleBind(msg *pgproto3.Bind, preparedStatement *PreparedStatement) error {
	if err := preparedStatement.addParameters(*msg); err != nil {
		return err
	}
	qe.mb.queueMessages(&pgproto3.BindComplete{})
	return nil
}

func (qe *QueryExecutor) executeQuery(query string) (rows *sql.Rows, err error) {
	isData := qe.queryUtil.isDataQuery(query, qe.synchronizedSchemas)
	if isData {
		Logger.Info("Route query to vertica", "query", query)
		rewrittenQuery := qe.queryUtil.rewriteQuery(query)
		if rewrittenQuery != query {
			Logger.Info("Rewritten query", "query", rewrittenQuery)
		}
		rows, err = qe.conn.vdb.QueryContext(qe.ctx, rewrittenQuery)
	} else {
		Logger.Info("Route query to postgres", "query", query)
		rows, err = qe.conn.pgdb.QueryContext(qe.ctx, query)
	}
	return rows, err
}

func (qe *QueryExecutor) prepareStatement(preparedStatement *PreparedStatement, describe bool) (*sql.Stmt, error) {
	isData := qe.queryUtil.isDataQuery(preparedStatement.getQuery(), qe.synchronizedSchemas)
	var stmt *sql.Stmt
	var dberr error
	if isData {
		statementCp := PreparedStatement{
			name:          preparedStatement.name,
			query:         preparedStatement.query,
			parameterOIDs: preparedStatement.parameterOIDs,
			parameters:    preparedStatement.parameters,
			binds:         preparedStatement.binds,
		}
		if describe {
			statementCp.query = qe.queryUtil.limitQuery(statementCp.query, 1)
		}

		query := statementCp.query
		Logger.Info("Route query to vertica", "query", query)

		rewrittenQuery := qe.queryUtil.rewriteQuery(query)
		if rewrittenQuery != query {
			Logger.Info("Rewritten query", "query", rewrittenQuery)
		}
		stmt, dberr = qe.conn.vdb.PrepareContext(qe.ctx, rewrittenQuery)
	} else {
		query := preparedStatement.query
		Logger.Info("Route query to postgres", "query", query)
		stmt, dberr = qe.conn.pgdb.PrepareContext(qe.ctx, query)
	}

	return stmt, dberr
}

func (qe *QueryExecutor) queryStatement(stmt *sql.Stmt, preparedStatement *PreparedStatement) (*sql.Rows, []*sql.ColumnType, error) {
	var rows *sql.Rows
	var cols []*sql.ColumnType
	var dberr error
	binds := preparedStatement.getBinds()

	Logger.Info("Query statement", "query", preparedStatement.query, "binds", binds)

	if rows, dberr = stmt.QueryContext(qe.ctx, *binds...); dberr != nil {
		return nil, nil, dberr
	}
	if cols, dberr = rows.ColumnTypes(); dberr != nil {
		return nil, nil, dberr
	}

	return rows, cols, nil
}

func (qe *QueryExecutor) executePreparedStatement(preparedStatement *PreparedStatement, describe bool) (*sql.Rows, []*sql.ColumnType, error) {
	stmt, err := qe.prepareStatement(preparedStatement, describe)
	if err != nil {
		return nil, nil, err
	}

	rows, cols, err := qe.queryStatement(stmt, preparedStatement)
	if err != nil {
		return nil, nil, err
	}

	return rows, cols, nil
}

func (qe *QueryExecutor) handleDescribe(msg *pgproto3.Describe) error {
	var preparedStatement *PreparedStatement
	if qe.currPreparedStatement == nil {
		if ps, ok := qe.preparedStatements[msg.Name]; ok {
			preparedStatement = ps
		} else {
			return fmt.Errorf("unknown prepared statement: %s", msg.Name)
		}
	} else {
		preparedStatement = qe.currPreparedStatement
	}

	query := preparedStatement.getQuery()
	if qe.queryUtil.queryReturnsNoRows(query) || qe.queryUtil.queryShouldReturnEmptyResponse(query) || qe.queryUtil.isDeclareCursorQuery(query) {
		switch msg.ObjectType {
		case 'S':
			qe.mb.queueMessages(&pgproto3.ParameterDescription{}, &pgproto3.NoData{})
			return nil
		case 'P':
			qe.mb.queueMessages(&pgproto3.NoData{})
			return nil
		default:
			return fmt.Errorf("unexpected object type: %c", msg.ObjectType)
		}
	}

	rows, cols, err := qe.executePreparedStatement(preparedStatement, true)
	if err != nil {
		qe.mb.queueMessages(
			qe.getErrorResponse(err),
		)
		qe.mb.sendQueuedMessages()
		return fmt.Errorf("exec: %w", err)
	}

	defer rows.Close()
	switch msg.ObjectType {
	case 'S':
		qe.mb.queueMessages(
			&pgproto3.ParameterDescription{ParameterOIDs: preparedStatement.parameterOIDs},
			toRowDescription(cols),
		)
	case 'P':
		qe.mb.queueMessages(
			toRowDescription(cols),
		)
	default:
		return fmt.Errorf("unexpected object type: %c", msg.ObjectType)
	}
	return nil
}

func (qe *QueryExecutor) handleExecute(preparedStatement *PreparedStatement) error {
	query := preparedStatement.getQuery()
	commandTag := getCommandTag(query)

	if qe.queryUtil.isDeallocateQuery(query) {
		qe.mb.queueMessages(
			&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
		return nil
	}

	if qe.queryUtil.isCloseQuery(query) {
		qe.closeCursor(query)
		qe.mb.queueMessages(
			&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
			&pgproto3.ReadyForQuery{TxStatus: qe.getTransactionStatus()},
		)
		return nil
	}

	if qe.queryUtil.isDeclareCursorQuery(query) {
		qe.declareCursor(query)
		return nil
	}

	if qe.queryUtil.isFetchQuery(query) {
		qe.fetchFromCursor(query)
		return nil
	}

	rows, cols, err := qe.executePreparedStatement(preparedStatement, false)

	if err != nil {
		qe.mb.queueMessages(
			qe.getErrorResponse(err),
		)

		qe.mb.sendQueuedMessages()
		return err
	}

	defer rows.Close()

	if qe.queryUtil.isBeginQuery(query) {
		qe.inTransaction = true
	}

	if qe.queryUtil.queryDiscardsTransaction(query) {
		qe.inTransaction = false
	}

	if qe.queryUtil.isSetQuery(query) {
		param, value, parseErr := parseSetQuery(query)
		if parseErr != nil {
			Logger.Error("Error parsing set query", "query", query, "error", parseErr)
			qe.mb.queueMessages(&pgproto3.CommandComplete{CommandTag: []byte(commandTag)})
			return nil
		} else {
			qe.mb.queueMessages(
				&pgproto3.ParameterStatus{Name: param, Value: value},
				&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
			)
			return nil
		}
	}

	if qe.queryUtil.queryReturnsNoRows(query) {
		qe.mb.queueMessages(
			&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
		)
		return nil
	}

	if qe.queryUtil.queryShouldReturnEmptyResponse(query) {
		qe.mb.queueMessages(
			&pgproto3.EmptyQueryResponse{},
			&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
		)
		return nil
	}

	err = qe.writeRowsInChunks(rows, cols)
	if err != nil {
		return err
	}

	qe.mb.queueMessages(
		&pgproto3.CommandComplete{CommandTag: []byte(commandTag)},
	)
	return err
}

func (qe *QueryExecutor) writeRowsInChunks(rows *sql.Rows, cols []*sql.ColumnType) error {
	if err := qe.mb.sendQueuedMessages(); err != nil {
		return err
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	for rows.Next() {
		row, err := scanRowToText(rows, cols)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		qe.mb.queueMessages(row)
		if qe.mb.buffSize() >= qe.maxBufferSize {
			if err := qe.mb.sendQueuedMessages(); err != nil {
				return err
			}
		}
	}
	if qe.mb.buffSize() > 0 {
		return qe.mb.sendQueuedMessages()
	}
	return nil
}
