# Tezos Delegation Service

Technical test for Kiln

This service gather new [baking delegations](https://opentezos.com/node-baking/baking/delegating) made on the Tezos protocol and expose them through an API.

## Requirements

This project creates and manage its own local instance of SQLite3 via [mattn's driver](https://github.com/mattn/go-sqlite3).

#### Important:

Because this is a CGO enabled package, you are required to set the environment variable `CGO_ENABLED=1` and have a `gcc` compiler present within your path.

## Run

### Makefile

Makefile contains a list of default use cases

```make
build:
    build the project to ./bin/tds
run:
    starts the built project with default configuration
dev:
    starts the project in developpment/debug mode
test:
    run all go tests with coverage profiling
empty:
    empty the store
sync:
    fill the store with all historical delegation events
```

### Manual

The main project is `cmd/tds` and comes with the following options

```go
$ go run ./cmd/tds -h
    -api string
            tzkt api delegation endpoint (default "https://api.tzkt.io/v1/operations/delegations")
    -db string
            path to the database file (default "delegations.db")
    -debug
            enable debug logging
    -nohistory
            disable history sync
    -port int
            http server port (default 8080)
    -sync string
            sync interval, should be a duration string (default "1m")
```

To manipulate the store directly we use `cmd/db` (defaule behavior is to fill the store with historical data)

```go
$ go run ./cmd/db -h
    -api string
            tzkt api delegation endpoint (default "https://api.tzkt.io/v1/operations/delegations")
    -db string
            path to the database file (default "delegations.db")
    -debug
            enable debug logging
    -empty
            empty the database
```

## Endpoints

The app exposes 1 endpoint.

### `GET  /xtz/delegations`

Returns the delegations of the current year, ordered by descending timestamps

#### Query parameters:

- `year=YYYY`: (Optional) returns the delegations of the given year.

#### Returns

```json
{
  "data": [
    {
      "timestamp": "2024-10-31T10:14:05Z",
      "delegator": "tz1KhjVyh7yc6197M5iZqnpn7u7aDSoqWB4R",
      "amount": "2327823247",
      "level": "6993511"
    },
    {
      "timestamp": "2024-10-31T10:02:05Z",
      "delegator": "tz1P9h5zJoaho148uXCv1iMsum76Rr9LJbGg",
      "amount": "109795184",
      "level": "6993439"
    }
  ]
}
```
