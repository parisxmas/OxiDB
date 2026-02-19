package middleware

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
	user   string
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// SetUser is called by auth middleware to tag the request with the authenticated user.
func (w *statusWriter) SetUser(u string) {
	w.user = u
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ip := strings.SplitN(xff, ",", 2)[0]; ip != "" {
			return strings.TrimSpace(ip)
		}
	}
	if xri := r.Header.Get("X-Real-Ip"); xri != "" {
		return xri
	}
	return r.RemoteAddr
}

func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		dur := time.Since(start).Round(time.Millisecond)

		uri := r.URL.Path
		if r.URL.RawQuery != "" {
			uri = uri + "?" + r.URL.RawQuery
		}

		user := "-"
		if sw.user != "" {
			user = sw.user
		}

		log.Printf("%s %s %d %dB %s | ip=%s user=%s ua=%q",
			r.Method, uri, sw.status, sw.bytes, dur,
			clientIP(r), user, r.Header.Get("User-Agent"))
	})
}
