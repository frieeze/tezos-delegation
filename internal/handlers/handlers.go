package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/frieeze/tezos-delegation/internal/store"
	"github.com/rs/zerolog/log"
)

// Handlers is a struct that holds all  http handlers
type Handlers struct {
	Store store.Store
}

type ErrorResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

// writeError logs and render the error
func writeError(w http.ResponseWriter, r *http.Request, err error, code int) {
	log.Ctx(r.Context()).Error().Err(err).Str("path", r.URL.Path).Msg("request failed")
	w.WriteHeader(code)
	writeJSON(w, ErrorResponse{Error: err.Error(), Code: code})

}

func writeJSON(w http.ResponseWriter, data interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(data)
}
