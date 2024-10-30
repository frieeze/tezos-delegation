package sync

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
	"github.com/rs/zerolog/log"
)

// NewLive creates a new live syncer
// It will sync the delegations from the given url every interval
// and store them in the given store
func NewLive(api string, interval time.Duration, s store.Store) *Live {
	return &Live{
		api:      strings.TrimSuffix(api, "/"),
		interval: interval,
		store:    s,
	}
}

type Live struct {
	api      string
	interval time.Duration
	store    store.Store

	ctx    context.Context
	cancel context.CancelFunc
	ticker *time.Ticker
	last   time.Time
	to     string

	stopped chan bool
}

const dateFormat = "2006-01-02T15:04:05Z"

var (
	ErrNoInterval = errors.New("no interval")
)

// Sync will start syncing the delegations
// It will sync the delegations every interval
// and store them in the store
// from is optional and will be used to start syncing from a specific date
// Returns an error if the first sync sync fails
func (l *Live) Sync(ctx context.Context, from string) error {
	if l.interval == 0 {
		return ErrNoInterval
	}
	l.ctx, l.cancel = context.WithCancel(ctx)
	l.ticker = time.NewTicker(l.interval)
	l.last = time.Now()

	if from != "" {
		last, err := time.Parse(dateFormat, from)
		if err != nil {
			return err
		}
		l.last = last
	}

	log.Ctx(ctx).Info().Str("from", l.last.Format(dateFormat)).Msg("start live sync")

	err := l.sync()
	if err != nil {
		return err
	}
	go func() {
		l.stopped = make(chan bool, 1)
		defer func() { l.stopped <- true }()
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.ticker.C:
				err := l.sync()
				if err != nil {
					log.Ctx(ctx).Error().Err(err).Msg("failed to sync")
				}
			}
		}
	}()
	return nil
}

// Stop will stop the syncing
func (l Live) Stop() {
	if l.ctx == nil {
		return
	}
	l.cancel()
	l.ticker.Stop()

	// Wait for the sync to stop
	if l.stopped != nil {
		<-l.stopped
		close(l.stopped)
	}
}

func (l *Live) sync() error {
	log.Ctx(l.ctx).Debug().Msg("sync live")
	delegations, err := getDelegations(l.ctx, l.api, getOpts{
		// Get delegations from the last interval with 20% overlap
		TsGe: l.last.Add(-(l.interval / 5)).Format(dateFormat),
		TsLt: l.to,
	})
	if err != nil {
		return err
	}

	l.last = time.Now()

	if len(delegations) == 0 {
		return nil
	}
	log.Ctx(l.ctx).Debug().Int("delegations", len(delegations)).Msg("insert delegations")
	return l.store.Insert(l.ctx, delegations)
}

type History struct {
	api   string
	store store.Store

	ctx    context.Context
	cancel context.CancelFunc

	stopped chan bool
}

// NewHistory creates a new history syncer
// It will sync the delegations from the given url
// and store them in the given store
func NewHistory(api string, s store.Store) *History {
	return &History{
		api:   api,
		store: s,
	}
}

// First delegation event from tzkt's API
const firstDelegation = "2018-06-30T19:30:27Z"

func (h *History) Stop() {
	if h.ctx == nil {
		return
	}
	h.cancel()

	// Wait for the sync to stop
	if h.stopped != nil {
		<-h.stopped
		close(h.stopped)
	}
}

// Sync will start syncing the delegations
// from and to are optional and will be used to filter the delegations
// returns the timestamp of the last delegation
// dates should be in RFC3339 format
func (h *History) Sync(ctx context.Context, from, to string) error {
	if from == "" {
		log.Ctx(ctx).Debug().Msg("no start date provided")
		storeLast, err := h.store.LastDelegation(ctx)
		if err != nil {
			return fmt.Errorf("failed to get last delegation: %w", err)
		}
		if storeLast != nil {
			from = storeLast.Timestamp
		} else {
			from = firstDelegation
		}
		log.Ctx(ctx).Debug().Str("from", from).Msg("new start date")
	}
	if to == "" {
		to = time.Now().Format(dateFormat)
		log.Ctx(ctx).Debug().Msg("no end date provided")
		log.Ctx(ctx).Debug().Str("to", to).Msg("new end date")
	}

	log.Ctx(ctx).Info().Str("from", from).Str("to", to).Msg("sync history")

	h.stopped = make(chan bool, 1)
	defer func() { h.stopped <- true }()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		log.Ctx(ctx).Debug().Str("from", from).Str("to", to).Msg("new batch")
		last, err := h.batch(ctx, from, to)
		if err != nil {
			return err
		}
		// No more delegations
		if last == "" || last > to {
			return nil
		}
		from = last
	}
}

func (h *History) batch(ctx context.Context, from, to string) (string, error) {
	delegations, err := getDelegations(ctx, h.api, getOpts{
		TsGe:  from,
		TsLt:  to,
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

	client := http.Client{
		Timeout: 2 * time.Second,
	}

	resp, err := client.Do(req)
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
