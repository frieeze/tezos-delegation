package middleware

import (
	"net/http"
	"time"

	"github.com/rs/zerolog/hlog"
)

type Middleware func(http.Handler) http.Handler

func Use(mw ...Middleware) Middleware {
	return func(next http.Handler) http.Handler {
		for _, m := range mw {
			next = m(next)
		}
		return next
	}
}

func Logger() Middleware {
	return hlog.AccessHandler(
		func(r *http.Request, status, size int, duration time.Duration) {
			duration = duration.Truncate(time.Millisecond)
			dur := duration.String()
			if status > 299 {
				hlog.FromRequest(r).Error().
					Str("method", r.Method).
					Int("status", status).
					Stringer("url", r.URL).
					Str("duration", dur).
					Msg("")
				return
			}
			hlog.FromRequest(r).Info().
				Str("method", r.Method).
				Int("status", status).
				Stringer("url", r.URL).
				Str("duration", dur).
				Msg("")
		})
}
