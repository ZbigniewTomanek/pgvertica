#!/bin/bash

set -o pipefail

version="$(cat version)"

if ! command -v go &>/dev/null; then
  echo "go binary is not on path!"
  exit 1
fi

echo "Start local postgres instance"
docker run -d --rm \
  -p 5431:5432 \
  --name pgvertica_postgres \
  -e POSTGRES_USER=pgvertica_user \
  -e POSTGRES_PASSWORD='pgvertica-secret-password!' \
  -e POSTGRES_DB=pgvertica \
  postgres:latest
POSTGRES_CONN="postgresql://pgvertica_user:pgvertica-secret-password!@localhost:5431/pgvertica?sslmode=disable"

echo "Build proxy binary"
go build -o pgvertica ./cmd/pgvertica/main.go

until docker exec pgvertica_postgres pg_isready -h localhost -p 5432 -U pgvertica_user; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

echo "Start PGVertica proxy version: $version"
./pgvertica --pgconn "$POSTGRES_CONN" "$@" ; docker rm -f pgvertica_postgres
