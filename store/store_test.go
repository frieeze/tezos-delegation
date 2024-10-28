package store

import (
	"os"
	"testing"
	"time"

	tds "github.com/frieeze/tezos-delegation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prepareFile(content string) (string, error) {
	f, err := os.CreateTemp("", "test")
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	if err != nil {
		return "", err
	}
	return f.Name(), nil
}

func removeFiles(f string) {
	os.Remove(f)
}

func Test_fileLastDate_ok(t *testing.T) {
	del := tds.Delegation{
		Timestamp: "2018-06-30T19:30:27Z",
		Delegator: "tz1Wit2PqodvPeuRRhdQXmkrtU8e8bRYZecd",
		Amount:    "25079312620",
		Level:     "6961961",
	}
	f, err := prepareFile(del.CSV())
	require.NoError(t, err)
	defer removeFiles(f)

	resp, err := fileLastDate(f)
	require.NoError(t, err)
	expected, _ := time.Parse(time.RFC3339, del.Timestamp)
	assert.Equal(t, expected, resp)
}

func Test_fileLastDate_empty(t *testing.T) {
	f, err := prepareFile("")
	require.NoError(t, err)
	defer removeFiles(f)

	_, err = fileLastDate(f)
	assert.ErrorIs(t, err, errFileEmpty)
}

func Test_fileLastDate_noFile(t *testing.T) {
	_, err := fileLastDate("no-file")
	assert.ErrorIs(t, err, os.ErrNotExist)
}
