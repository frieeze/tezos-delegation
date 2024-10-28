package store

import (
	"context"
	"database/sql"

	tds "github.com/frieeze/tezos-delegation"
)

type Store interface {
	Insert(ctx context.Context, ds []tds.Delegation) error
	GetByYear(ctx context.Context, year string) ([]tds.Delegation, error)
	Close() error
}

type sqlite struct {
	db *sql.DB
}

func NewSqLite(path string) (Store, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	return &sqlite{
		db: db,
	}, nil
}

func (s *sqlite) Insert(ctx context.Context, ds []tds.Delegation) error {
	const query = `
	INSERT INTO delegations (level, delegator, amount, timestamp, id)
	VALUES (?, ?, ?, ?, ?);
	`
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, d := range ds {
		_, err = stmt.Exec(ctx, d.Level, d.Delegator, d.Amount, d.Timestamp, d.Id)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s sqlite) GetByYear(ctx context.Context, year string) ([]tds.Delegation, error) {
	const query = `
	SELECT level, delegator, amount, timestamp, id
	FROM delegations
	WHERE timestamp LIKE ?
	ORDER BY timestamp DESC;
	`
	rows, err := s.db.QueryContext(ctx, query, year+"%")
	if err != nil {
		return nil, err
	}

	var delegations []tds.Delegation
	for rows.Next() {
		var d tds.Delegation
		err = rows.Scan(
			&d.Level,
			&d.Delegator,
			&d.Amount,
			&d.Timestamp,
			&d.Id,
		)
		if err != nil {
			return nil, err
		}
		delegations = append(delegations, d)
	}
	return delegations, nil
}

func (s *sqlite) Close() error {
	return s.db.Close()
}

func (s *sqlite) createTable() error {
	const query = `
	CREATE TABLE IF NOT EXISTS delegations (
		pk        INTEGER PRIMARY KEY AUTOINCREMENT,
		id	  TEXT UNIQUE,
		level     TEXT NOT NULL,
		delegator TEXT NOT NULL,
		amount    TEXT NOT NULL,
		timestamp TEXT NOT NULL
	);
	`
	_, err := s.db.Exec(query)
	return err
}
