package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	tds "github.com/frieeze/tezos-delegation"
	_ "github.com/mattn/go-sqlite3"
)

// Store is the interface that wraps the basic store methods.
type Store interface {
	// Insert adds delegations to the store.
	Insert(ctx context.Context, ds []tds.Delegation) error
	// GetByYear returns all delegations for a given year, ordered by descending timestamps.
	GetByYear(ctx context.Context, year string) ([]tds.Delegation, error)
	// LastDelegation returns the last delegation by timestamp.
	LastDelegation(ctx context.Context) (*tds.Delegation, error)
	// Empty deletes all delegations from the store.
	Empty(ctx context.Context) error
	// Close the store.
	Close() error
}

type sqlite struct {
	db *sql.DB
}

// NewSqLite creates a new SQLite3 store.
// If the database file does not exist, it will be created.
func NewSqLite(ctx context.Context, path string) (Store, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open database file: %w", err)
	}
	f.Close()

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	store := &sqlite{
		db: db,
	}

	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	err = store.createTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	return store, nil
}

// Insert adds delegations to the database.
// If a delegation with the same id already exists, it will be ignored.
func (s *sqlite) Insert(ctx context.Context, ds []tds.Delegation) error {
	if len(ds) == 0 {
		return nil
	}
	const query = `
	INSERT INTO delegations (level, delegator, amount, timestamp, id)
	VALUES (?, ?, ?, ?, ?);
	`
	tx, err := s.db.Begin()
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
		_, err = stmt.ExecContext(ctx, d.Level, d.Delegator, d.Amount, d.Timestamp, d.ID)
		if err != nil && !isUniqueViolation(err) {
			return err
		}
	}
	return tx.Commit()
}
func isUniqueViolation(err error) bool {
	return err.Error() == "UNIQUE constraint failed: delegations.id"
}

// GetByYear returns all delegations for a given year.
// Delegations are ordered by timestamp in descending order.
// The year should be in the format "2006".
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

	var delegations = []tds.Delegation{}
	for rows.Next() {
		var d tds.Delegation
		err = rows.Scan(
			&d.Level,
			&d.Delegator,
			&d.Amount,
			&d.Timestamp,
			&d.ID,
		)
		if err != nil {
			return nil, err
		}
		delegations = append(delegations, d)
	}
	return delegations, nil
}

// LastDelegation returns the last delegation by timestamp.
func (s sqlite) LastDelegation(ctx context.Context) (*tds.Delegation, error) {
	const query = `
	SELECT level, delegator, amount, timestamp, id
	FROM delegations
	ORDER BY timestamp DESC
	LIMIT 1;
	`
	var d tds.Delegation
	err := s.db.QueryRowContext(ctx, query).Scan(
		&d.Level,
		&d.Delegator,
		&d.Amount,
		&d.Timestamp,
		&d.ID,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &d, err
}

// Close closes the database connection.
func (s *sqlite) Close() error {
	return s.db.Close()
}

// Empty deletes all delegations from the database.
func (s *sqlite) Empty(ctx context.Context) error {
	const query = `DELETE FROM delegations;`
	_, err := s.db.ExecContext(ctx, query)
	return err
}

func (s *sqlite) createTable(ctx context.Context) error {
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
	_, err := s.db.ExecContext(ctx, query)
	return err
}
