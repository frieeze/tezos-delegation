package tzs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	tds "github.com/frieeze/tezos-delegation"
	"github.com/frieeze/tezos-delegation/internal/store"
)

type Syncer interface {
	Sync(ctx context.Context, from, to string) error
	Stop()
}

const defaultApi = "https://api.tzkt.io/v1/operations/delegations"

// NewLive creates a new live syncer
// It will sync the delegations from the given url every interval
// and store them in the given store
func NewLive(api string, interval time.Duration, s store.Store) Syncer {
	if api == "" {
		api = defaultApi
	}
	return &live{
		api:      strings.TrimSuffix(api, "/"),
		interval: interval,
		store:    s,
	}
}

type live struct {
	api      string
	interval time.Duration
	store    store.Store

	ctx    context.Context
	cancel context.CancelFunc
	ticker *time.Ticker
	last   time.Time
	to     string
}

// Sync will start syncing the delegations
// It will sync the delegations every interval
// and store them in the store
// from and to are optional and will be used to aim at a specific range
// Returns an error if the first sync sync fails
func (l *live) Sync(ctx context.Context, from, to string) error {
	l.ctx, l.cancel = context.WithCancel(ctx)
	l.ticker = time.NewTicker(l.interval)
	l.last = time.Now()
	if from != "" {
		last, err := time.Parse(time.RFC3339, from)
		if err != nil {
			return err
		}
		l.last = last
	}

	l.to = to

	err := l.sync()
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.ticker.C:
			}
		}
	}()
	return nil
}

// Stop will stop the syncing
func (l live) Stop() {
	if l.ctx == nil {
		return
	}
	l.cancel()
	l.ticker.Stop()
}

func (l *live) sync() error {
	delegations, err := getDelegations(l.ctx, l.api, getOpts{
		// Get delegations from the last interval with 20% overlap
		TsGe: l.last.Add(-(l.interval / 5)).Format(time.RFC3339),
		TsLt: l.to,
	})
	if err != nil {
		return err
	}

	l.last = time.Now()

	if len(delegations) == 0 {
		return nil
	}
	return l.store.Insert(l.ctx, delegations)
}

type history struct {
	api   string
	store store.Store

	from string
	to   string

	ctx    context.Context
	cancel context.CancelFunc
}

// NewHistory creates a new history syncer
// It will sync the delegations from the given url
// and store them in the given store
func NewHistory(api string, s store.Store) Syncer {
	if api == "" {
		api = defaultApi
	}
	return &history{
		api:   api,
		store: s,
	}
}

// First delegation event from tzkt's API
const firstDelegation = "2018-06-30T19:30:27Z"

func (h *history) Stop() {
	if h.ctx == nil {
		return
	}
	h.cancel()
}

// Sync will start syncing the delegations
// from and to are optional and will be used to filter the delegations
// dates should be in RFC3339 format
func (h *history) Sync(ctx context.Context, from, to string) error {
	if from == "" || firstDelegation > from {
		from = firstDelegation
	}

	for {
		last, err := h.batch(h.ctx, h.from)
		if err != nil {
			return err
		}
		// No more delegations
		if last == "" || last > h.to {
			return nil
		}
		h.from = last
	}
}

func (h *history) batch(ctx context.Context, from string) (string, error) {
	delegations, err := getDelegations(ctx, h.api, getOpts{
		TsGe:  from,
		TsLt:  h.to,
		Limit: 10000,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get delegations: %w", err)
	}

	err = h.store.Insert(ctx, delegations)
	if err != nil {
		return "", fmt.Errorf("failed to insert delegations: %w", err)
	}

	// No more delegations
	if len(delegations) < 10000 {
		return "", nil
	}

	return delegations[len(delegations)-1].Timestamp, nil

}

type getOpts struct {
	TsGe  string
	TsLt  string
	Limit int
}

var (
	ErrInvalidStatusCode = errors.New("invalid status code")
)

func getDelegations(ctx context.Context, url string, opts getOpts) ([]tds.Delegation, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		"GET",
		url,
		nil,
	)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("select", "timestamp,sender,amount,level,id")
	if opts.TsGe != "" {
		q.Add("timestamp.ge", opts.TsGe)
	}
	if opts.TsLt != "" {
		q.Add("timestamp.lt", opts.TsLt)
	}
	if opts.Limit > 0 {
		q.Add("limit", strconv.Itoa(opts.Limit))
	}
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w : %d", ErrInvalidStatusCode, resp.StatusCode)
	}

	return decodeDelegations(resp.Body, opts.Limit)
}

type responseDelegation struct {
	Timestamp string `json:"timestamp"`
	Sender    struct {
		Address string `json:"address"`
	} `json:"sender"`
	Amount int `json:"amount"`
	Level  int `json:"level"`
	Id     int `json:"id"`
}

// capacity is used to preallocate the slice
// to avoid reallocations
func decodeDelegations(raw io.Reader, capacity int) ([]tds.Delegation, error) {
	var delegations = make([]tds.Delegation, 0, capacity)
	dec := json.NewDecoder(raw)

	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return nil, err
	}
	for dec.More() {
		var d responseDelegation
		if err := dec.Decode(&d); err != nil {
			return nil, err
		}
		delegations = append(delegations, tds.Delegation{
			Timestamp: d.Timestamp,
			Delegator: d.Sender.Address,
			Amount:    strconv.Itoa(d.Amount),
			Level:     strconv.Itoa(d.Level),
			Id:        strconv.Itoa(d.Id),
		})
	}
	return delegations, nil
}
