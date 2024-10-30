package handlers

import (
	"net/http"
	"time"
)

// AddXTZRoutes adds all the routes for the XTZ API
func (h *Handlers) AddXTZRoutes() *http.ServeMux {
	r := http.NewServeMux()
	r.HandleFunc("GET /delegations", h.Delegations)

	return r
}

// Delegations returns all delegations for a given year
// or the current year if no year is provided.
func (h *Handlers) Delegations(w http.ResponseWriter, r *http.Request) {
	// get year from query
	year := r.URL.Query().Get("year")
	if year == "" {
		year = time.Now().Format("2006")
	}

	// get delegations
	delegations, err := h.Store.GetByYear(r.Context(), year)
	if err != nil {
		writeError(w, r, err, http.StatusInternalServerError)
		return
	}

	// render delegations
	err = writeJSON(w, delegations)
	if err != nil {
		writeError(w, r, err, http.StatusInternalServerError)
		return
	}
}
