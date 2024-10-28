package tzs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	tds "github.com/frieeze/tezos-delegation"
	"github.com/frieeze/tezos-delegation/store"
)

type Syncer interface {
	Sync(ctx context.Context) error
	Stop()
}

func NewLive(url string, interval time.Duration, s store.Store) Syncer {
	return &live{
		url:      url,
		interval: interval,
		store:    s,
	}
}

type live struct {
	url      string
	interval time.Duration
	store    store.Store

	ctx    context.Context
	cancel context.CancelFunc
	ticker *time.Ticker
}

func (l *live) Sync(ctx context.Context) error {
	l.ctx, l.cancel = context.WithCancel(ctx)
	l.ticker = time.NewTicker(l.interval)

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

func (l live) Stop() {
	if l.ctx == nil {
		return
	}
	l.cancel()
	l.ticker.Stop()
}

func (l live) sync() error {
	delegations, err := getDelegations(l.ctx, l.url, GetOpts{
		// Get delegations from the last interval with 20% overlap
		TsGe: time.Now().Sub(l.interval + l.interval/5).Format("2006-01-02T15:04:05Z"),
	})
	if err != nil {
		return err
	}
	if len(delegations) == 0 {
		return nil
	}
	return l.store.InsertBatch(l.ctx, delegations)
}

type GetOpts struct {
	TsGe  time.Time
	TsLt  time.Time
	Limit int
}

var (
	ErrInvalidStatusCode = errors.New("invalid status code")
)

type responseDelegation struct {
	Timestamp string `json:"timestamp"`
	Sender    struct {
		Address string `json:"address"`
	} `json:"sender"`
	Amount string `json:"amount"`
	Level  int    `json:"level"`
	Id     int    `json:"id"`
}

func getDelegations(ctx context.Context, url string, opts GetOpts) ([]tds.Delegation, error) {
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
	if !opts.TsGe.IsZero() {
		q.Add("timastamp.ge", opts.TsGe.Format("2006-01-02T15:04:05Z"))
	}
	if !opts.TsLt.IsZero() {
		q.Add("timastamp.lt", opts.TsLt.Format("2006-01-02T15:04:05Z"))
	}
	if opts.Limit > 0 {
		q.Add("limit", string(opts.Limit))
	}
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, ErrInvalidStatusCode
	}

	var delegations []tds.Delegation
	decoder := json.NewDecoder(resp.Body)
	for decoder.More() {
		var d responseDelegation
		if err := decoder.Decode(&d); err != nil {
			return nil, err
		}
		delegations = append(delegations, tds.Delegation{
			Timestamp: d.Timestamp,
			Delegator: d.Sender.Address,
			Amount:    d.Amount,
			Level:     strconv.Itoa(d.Level),
			Id:        strconv.Itoa(d.Id),
		})
	}

	return delegations, nil
}
