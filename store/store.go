package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	tds "github.com/frieeze/tezos-delegation"
)

type Store interface {
	Insert(ctx context.Context, ds []tds.Delegation) error
	GetByYear(ctx context.Context, year string) ([]tds.Delegation, error)
}

type fileStore struct {
	basepath string
}

func (s *fileStore) Insert(ctx context.Context, ds []tds.Delegation) error {
	slices.SortStableFunc(ds, func(i, j tds.Delegation) int {
		// sort by timestamp in ascending order
		return strings.Compare(i.Timestamp, j.Timestamp)
	})

	return nil
}

func delegationsByYear(ds []tds.Delegation) [][]tds.Delegation {
	// group delegations by year
	delegations := [][]tds.Delegation{}
	years := make(map[string]int)
	for _, d := range ds {
		year := d.Timestamp[:4]
		years[year] = append(years[year], d)
	}

	// convert map to slice
	var res [][]tds.Delegation
	for _, v := range years {
		res = append(res, v)
	}
	return res
}

var (
	errFileEmpty = errors.New("file is empty")
)

func (s *fileStore) append(path string, content []byte) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(content)
	if err != nil {
		return err
	}
	return nil
}

func (s fileStore) insert(path string, content []byte) error {
	return nil
}

func (s fileStore) fileName(year string) string {
	return s.basepath + "/" + year + ".csv"
}

// len("2018-06-30T19:30:27Z") == 20
const dateSize = 20

func fileLastDate(path string) (time.Time, error) {
	file, err := os.Open(path)
	if err != nil {
		return time.Time{}, err
	}
	defer file.Close()

	buf := make([]byte, 20)
	stat, err := file.Stat()
	if err != nil {
		return time.Time{}, err
	}

	// in CSV format the last element is the date
	// so we need to read the last 20 bytes
	// +1 offset for the newline
	offset := stat.Size() - (dateSize + 1)
	if offset < (dateSize + 1) {
		return time.Time{}, errFileEmpty
	}

	_, err = file.ReadAt(buf, offset)
	if err != nil {
		return time.Time{}, err
	}

	return time.Parse(time.RFC3339, string(buf))
}
