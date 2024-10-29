package store

import (
	"context"
	"database/sql"

	tds "github.com/frieeze/tezos-delegation"
	_ "github.com/mattn/go-sqlite3"
)

type Store interface {
	Insert(ctx context.Context, ds []tds.Delegation) error
	GetByYear(ctx context.Context, year string) ([]tds.Delegation, error)
	Length(ctx context.Context) (int, error)
	LastDelegation(ctx context.Context) (tds.Delegation, error)
	Close() error
}

type sqlite struct {
	*sql.DB
}

func NewSqLite(path string) (Store, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	store := &sqlite{
		db,
	}

	err = store.createTable()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (s *sqlite) Insert(ctx context.Context, ds []tds.Delegation) error {
	const query = `
	INSERT INTO delegations (level, delegator, amount, timestamp, id)
	VALUES (?, ?, ?, ?, ?);
	`
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, d := range ds {
		_, err = stmt.ExecContext(ctx, d.Level, d.Delegator, d.Amount, d.Timestamp, d.Id)
		if err != nil && !isUniqueViolation(err) {
			return err
		}
	}
	return tx.Commit()
}
func isUniqueViolation(err error) bool {
	return err.Error() == "UNIQUE constraint failed: delegations.id"
}

func (s sqlite) GetByYear(ctx context.Context, year string) ([]tds.Delegation, error) {
	const query = `
	SELECT level, delegator, amount, timestamp, id
	FROM delegations
	WHERE timestamp LIKE ?
		ORDER BY timestamp DESC;
		`
	rows, err := s.DB.QueryContext(ctx, query, year+"%")
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

func (s sqlite) Length(ctx context.Context) (int, error) {
	const query = `
	SELECT COUNT(*)
	FROM delegations;
	`
	var count int
	err := s.DB.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}
func (s sqlite) LastDelegation(ctx context.Context) (tds.Delegation, error) {
	const query = `
	SELECT level, delegator, amount, timestamp, id
	FROM delegations
	ORDER BY timestamp DESC
	LIMIT 1;
	`
	var d tds.Delegation
	err := s.DB.QueryRowContext(ctx, query).Scan(
		&d.Level,
		&d.Delegator,
		&d.Amount,
		&d.Timestamp,
		&d.Id,
	)
	return d, err
}

func (s *sqlite) Close() error {
	return s.DB.Close()
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
	_, err := s.DB.Exec(query)
	return err
}
