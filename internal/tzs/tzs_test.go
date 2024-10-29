package tzs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	tds "github.com/frieeze/tezos-delegation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	response = `
[{"timestamp":"2024-10-29T10:22:25Z","sender":{"address":"tz1L6FGN8F2o3j8CsGCoktiFDdDLkbECEDms"},"amount":13814013,"level":6976378,"id":1401626186219520},{"timestamp":"2024-10-29T10:10:00Z","sender":{"address":"tz29LqGEjCrSR1HFhzMoujZvXi5Rgdhxe7mP"},"amount":2548493,"level":6976305,"id":1401610442899456},{"timestamp":"2024-10-29T10:09:00Z","sender":{"address":"tz29LqGEjCrSR1HFhzMoujZvXi5Rgdhxe7mP"},"amount":2548751,"level":6976299,"id":1401609161539584}]
	`
	expected = []tds.Delegation{
		{
			Timestamp: "2024-10-29T10:22:25Z",
			Delegator: "tz1L6FGN8F2o3j8CsGCoktiFDdDLkbECEDms",
			Amount:    "13814013",
			Level:     "6976378",
			Id:        "1401626186219520",
		},
		{
			Timestamp: "2024-10-29T10:10:00Z",
			Delegator: "tz29LqGEjCrSR1HFhzMoujZvXi5Rgdhxe7mP",
			Amount:    "2548493",
			Level:     "6976305",
			Id:        "1401610442899456",
		},
		{
			Timestamp: "2024-10-29T10:09:00Z",
			Delegator: "tz29LqGEjCrSR1HFhzMoujZvXi5Rgdhxe7mP",
			Amount:    "2548751",
			Level:     "6976299",
			Id:        "1401609161539584",
		},
	}
)

func Test_decodeDelegations_ok(t *testing.T) {
	reader := strings.NewReader(response)
	ds, err := decodeDelegations(reader)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, ds)
}

func Test_decodeDelegations_error_BadJSON(t *testing.T) {
	reader := strings.NewReader(`[{"timestamp":"2024-10-29T10:22:25Z","sender":{"address":`)
	_, err := decodeDelegations(reader)
	assert.Error(t, err)
}

func httpTestServer(response string, code int, reqTests func(r *http.Request)) *httptest.Server {
	serv := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if reqTests != nil {
					reqTests(r)
				}
				w.WriteHeader(code)
				w.Write([]byte(response))
			}))
	return serv
}

func Test_getDelegations_ok(t *testing.T) {
	serv := httpTestServer(response, 200, func(r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "timestamp,sender,amount,level,id", r.URL.Query().Get("select"))
		assert.Empty(t, r.URL.Query().Get("timestamp.ge"))
		assert.Empty(t, r.URL.Query().Get("timestamp.lt"))
		assert.Empty(t, r.URL.Query().Get("limit"))
	})
	defer serv.Close()
	ds, err := getDelegations(context.Background(), serv.URL, GetOpts{})
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, ds)
}

func Test_getDelegation_Params(t *testing.T) {
	var (
		date    = time.Date(2024, 10, 29, 10, 22, 25, 0, time.UTC)
		dateStr = "2024-10-29T10:22:25Z"
	)

	serv := httpTestServer("[]", 200, func(r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "timestamp,sender,amount,level,id", r.URL.Query().Get("select"))
		assert.Equal(t, dateStr, r.URL.Query().Get("timestamp.ge"))
		assert.Equal(t, dateStr, r.URL.Query().Get("timestamp.lt"))
		assert.Equal(t, "1000", r.URL.Query().Get("limit"))
	})
	defer serv.Close()

	opts := GetOpts{
		TsLt:  date,
		TsGe:  date,
		Limit: 1000,
	}
	del, err := getDelegations(context.Background(), serv.URL, opts)
	assert.NoError(t, err)
	assert.Empty(t, del)
}

func Test_getDelegations_error_HttpCode(t *testing.T) {
	serv := httpTestServer(response, 400, nil)
	defer serv.Close()
	_, err := getDelegations(context.Background(), serv.URL, GetOpts{})
	assert.ErrorIs(t, err, ErrInvalidStatusCode)
}

func Test_getDelegations_error_BadURL(t *testing.T) {
	_, err := getDelegations(context.Background(), "", GetOpts{})
	assert.Error(t, err)
}

type mockStore struct {
	mock.Mock
}

func (m *mockStore) Insert(ctx context.Context, ds []tds.Delegation) error {
	args := m.Called(ctx, ds)
	return args.Error(0)
}

func (m *mockStore) GetByYear(ctx context.Context, year string) ([]tds.Delegation, error) {
	args := m.Called(ctx, year)
	return args.Get(0).([]tds.Delegation), args.Error(1)
}

func (m *mockStore) Length(ctx context.Context) (int, error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

func (m *mockStore) LastDelegation(ctx context.Context) (tds.Delegation, error) {
	args := m.Called(ctx)
	return args.Get(0).(tds.Delegation), args.Error(1)
}

func (m *mockStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

func Test_NewLive(t *testing.T) {
	storage := &mockStore{}
	s := NewLive("http://localhost:8080", 10*time.Second, storage)
	assert.NotNil(t, s)
	require.IsType(t, &live{}, s)

	l := s.(*live)
	assert.Equal(t, "http://localhost:8080", l.url)
	assert.Equal(t, 10*time.Second, l.interval)
	assert.Equal(t, storage, l.store)
}
