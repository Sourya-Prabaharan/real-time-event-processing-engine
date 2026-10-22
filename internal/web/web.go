package web

import (
	_ "embed"
	"net/http"
)

//go:embed index.html
var IndexHTML []byte

// Handler serves the operational engineering dashboard at / and /dashboard.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/dashboard" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(IndexHTML)
	})
}
