package store

import (
	"context"
	"database/sql"
	"os"
	"testing"

	tds "github.com/frieeze/tezos-delegation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	delegations = []tds.Delegation{
		{
			Timestamp: "2020-10-29T10:22:25Z",
			Delegator: "tz1L6FGN8F2o3j8CsGCoktiFDdDLkbECEDms",
			Amount:    "13814013",
			Level:     "6976378",
			ID:        "1401626186219520",
		},
		{
			Timestamp: "2021-10-29T10:10:00Z",
			Delegator: "tz29LqGEjCrSR1HFhzMoujZvXi5Rgdhxe7mP",
			Amount:    "2548493",
			Level:     "6976305",
			ID:        "1401610442899456",
		},
		{
			Timestamp: "2022-10-29T10:09:00Z",
			Delegator: "tz29LqGEjCrSR1HFhzMoujZvXi5Rgdhxe7mP",
			Amount:    "2548751",
			Level:     "6976299",
			ID:        "1401609161539584",
		},
	}
)

const path = "test.db"

func prepareDB(t *testing.T) (Store, string) {
	s, err := NewSqLite(context.Background(), path)
	require.NoError(t, err)

	err = s.Insert(context.Background(), delegations)
	require.NoError(t, err)

	return s, path
}

func cleanupDB(t *testing.T, s Store, path string) {
	err := s.Close()
	require.NoError(t, err)

	err = os.Remove(path)
	require.NoError(t, err)
}

func queryTable(ctx context.Context, db *sql.DB) (string, error) {
	const query = "SELECT name FROM sqlite_master WHERE type='table';"
	var table string
	err := db.QueryRowContext(ctx, query).Scan(&table)
	return table, err
}

func Test_NewSqLite(t *testing.T) {
	s, err := NewSqLite(context.Background(), path)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	defer cleanupDB(t, s, path)

	table, err := queryTable(context.Background(), s.(*sqlite).db)
	require.NoError(t, err)
	assert.Equal(t, "delegations", table)
}

func length(db *sql.DB) (int, error) {
	const query = "SELECT COUNT(*) FROM delegations;"
	var count int
	err := db.QueryRow(query).Scan(&count)
	return count, err
}

func Test_sqlite_Insert(t *testing.T) {
	s, path := prepareDB(t)
	defer cleanupDB(t, s, path)

	count, err := length(s.(*sqlite).db)
	require.NoError(t, err)
	assert.Equal(t, len(delegations), count)

	// Test duplicate handling
	err = s.Insert(context.Background(), delegations)
	assert.NoError(t, err)

	count, err = length(s.(*sqlite).db)
	require.NoError(t, err)
	assert.Equal(t, len(delegations), count)
}

func Test_sqlite_GetByYear(t *testing.T) {
	s, path := prepareDB(t)
	defer cleanupDB(t, s, path)

	ds, err := s.GetByYear(context.Background(), "2021")
	require.NoError(t, err)
	assert.Len(t, ds, 1)
	assert.Equal(t, delegations[1], ds[0])
}

func Test_sqlite_GetByYear_Empty(t *testing.T) {
	s, path := prepareDB(t)
	defer cleanupDB(t, s, path)

	ds, err := s.GetByYear(context.Background(), "2000")
	require.NoError(t, err)
	require.NotNil(t, ds)
	assert.Len(t, ds, 0)
}

func Test_sqlite_LastDelegation(t *testing.T) {
	s, path := prepareDB(t)
	defer cleanupDB(t, s, path)

	d, err := s.LastDelegation(context.Background())
	require.NoError(t, err)
	assert.Equal(t, delegations[2], *d)
}

func Test_sqlite_LastDelegation_Empty(t *testing.T) {
	s, err := NewSqLite(context.Background(), path)
	require.NoError(t, err)
	defer cleanupDB(t, s, path)

	d, err := s.LastDelegation(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, d)
}

func Test_sqlite_Drop(t *testing.T) {
	s, path := prepareDB(t)
	defer cleanupDB(t, s, path)

	err := s.Empty(context.Background())
	require.NoError(t, err)

	count, err := length(s.(*sqlite).db)
	require.NoError(t, err)
	assert.Zero(t, count)
}
