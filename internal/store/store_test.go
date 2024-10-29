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

func newEmptyDB() (string, error) {
	file, err := os.CreateTemp("", "tds_test_db")
	if err != nil {
		return "", err
	}
	return file.Name(), nil
}

func queryTable(ctx context.Context, db *sql.DB) (string, error) {
	const query = "SELECT name FROM sqlite_master WHERE type='table';"
	var table string
	err := db.QueryRowContext(ctx, query).Scan(&table)
	return table, err
}

func Test_NewSqLite(t *testing.T) {
	path, err := newEmptyDB()
	require.NoError(t, err)
	defer os.Remove(path)

	s, err := NewSqLite(path)
	assert.NoError(t, err)
	assert.NotNil(t, s)

	table, err := queryTable(context.Background(), s.(*sqlite).DB)
	require.NoError(t, err)
	assert.Equal(t, "delegations", table)

	err = s.Close()
	assert.NoError(t, err)
}

var (
	delegations = []tds.Delegation{
		{
			Timestamp: "2020-10-29T10:22:25Z",
			Delegator: "tz1L6FGN8F2o3j8CsGCoktiFDdDLkbECEDms",
			Amount:    "13814013",
			Level:     "6976378",
			Id:        "1401626186219520",
		},
		{
			Timestamp: "2021-10-29T10:10:00Z",
			Delegator: "tz29LqGEjCrSR1HFhzMoujZvXi5Rgdhxe7mP",
			Amount:    "2548493",
			Level:     "6976305",
			Id:        "1401610442899456",
		},
		{
			Timestamp: "2022-10-29T10:09:00Z",
			Delegator: "tz29LqGEjCrSR1HFhzMoujZvXi5Rgdhxe7mP",
			Amount:    "2548751",
			Level:     "6976299",
			Id:        "1401609161539584",
		},
	}
)

func prepareDB(t *testing.T) (Store, string) {
	path, err := newEmptyDB()
	require.NoError(t, err)

	s, err := NewSqLite(path)
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

func Test_sqlite_Insert(t *testing.T) {
	s, path := prepareDB(t)
	defer cleanupDB(t, s, path)

	count, err := s.Length(context.Background())
	require.NoError(t, err)
	assert.Equal(t, len(delegations), count)

	// Test duplicate handling
	err = s.Insert(context.Background(), delegations)
	assert.NoError(t, err)

	count, err = s.Length(context.Background())
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

func Test_sqlite_LastDelegation(t *testing.T) {
	s, path := prepareDB(t)
	defer cleanupDB(t, s, path)

	d, err := s.LastDelegation(context.Background())
	require.NoError(t, err)
	assert.Equal(t, delegations[2], d)
}
