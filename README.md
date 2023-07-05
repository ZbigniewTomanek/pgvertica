# PGVertica

PGVertica is a proxy server designed to be an interface to connect from BI tools to Vertica using PostgreSQL connectors.

This project utilizes the PostgreSQL wire protocol to direct data-centric queries towards a Vertica database while
managing all other queries, such as metadata retrievals from pg_catalog, INSERT operations to temporary tables, and
others, using a local PostgreSQL instance with recreated Vertica schemas metadata.

This project is built on top of the existing PostgreSQL-sqlite
proxy, [postlite](https://github.com/benbjohnson/postlite).
For interfacing with Vertica, we use the `github.com/vertica/vertica-sql-go` library, while the PostgreSQL protocol
implementation is taken care of by the `github.com/jackc/pgproto3/v2` library.

Supported features:

- TLS encryption
- automatic synchronization of selected schemas metadata, between Vertica and Postgres
- automatic synchronization of users and roles between Vertica and Postgres
- extensive support for prepared statements
- handling multiple datatypes using TEXT/BINARY protocol
- fetching data forward with explicitly declared cursors

**Note, that this proxy does not support**

- queries that add data to Vertica - all changes will only be reflected in the local postgres instance
- `SELECT` queries which uses PostgreSQL specific syntax, which is not supported by Vertica dialect.

## Supported clients

This proxy has been tested with following BI tools:

- DBeaver
- Tableau
- PowerBI
- Excel
- Apache Superset
- ArcGIS

## Usage

### Build PGVertica

```bash
go build -o pgvertica ./cmd/pgvertica/main.go
```

### Run tests

```bash
go test -v
```

### Run PGVertica

```bash
./pgvertica
```

```
Usage of ./pgvertica:
  -addr string
        proxy server bind address (default ":5432")
  -log-level string
        logger level (default "INFO")
  -pgconn string
        Postgres connection string
  -require-password
        whether this proxy should ask for password
  -schemas-sync-interval-s int
        time interval between schemas synchronization (default 60)
  -vconn string
        Vertica connection string in format vertica://user:password@addr:port/db
  -x509-cert-path string
        Path to SSL x509 cert file, if empty proxy won't support SSL

```

To build proxy and run docker with postgres you can use. Script will fill only `--pgconn` parameter, you need to pass
the
rest of parameters, eg.:

```bash
./start-proxy.sh --vconn "vertica://vuser:vpass@vertica-host:5433/dbname" --require-password
```

## Contribution Policy

PGVertica is open to code contributions for bug fixes & documentation fixes only.
Features carry a long-term maintenance burden so they will not be accepted at
this time. Please [submit an issue][new-issue] if you have a feature you'd like
to request.

[new-issue]: https://github.com/ZbigniewTomanek/pgvertica/issues/new

