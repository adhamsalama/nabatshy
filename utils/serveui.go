package utils

import (
	"embed"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
)

// ServeUI serves static UI files using chi router and embed.FS
func ServeUI(content embed.FS, uiDir string) {
	r := chi.NewRouter()
	// Serve static assets
	r.Get("/assets/*", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		filePath := uiDir + "/" + path

		data, err := content.ReadFile(filePath)
		if err != nil {
			fmt.Printf("read error: %v\n", err)
			http.NotFound(w, r)
			return
		}

		switch ext := filepath.Ext(filePath); ext {
		case ".js":
			w.Header().Set("Content-Type", "application/javascript")
		case ".css":
			w.Header().Set("Content-Type", "text/css")
		case ".html":
			w.Header().Set("Content-Type", "text/html")
		default:
			w.Header().Set("Content-Type", "application/octet-stream")
		}

		w.Write(data)
	})

	// Fallback for SPA routes: serve index.html
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		// Only serve index.html for routes without a file extension
		if filepath.Ext(r.URL.Path) == "" {
			indexPath := uiDir + "/index.html"
			data, err := content.ReadFile(indexPath)
			if err != nil {
				fmt.Printf("index read error: %v\n", err)
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			w.Write(data)
			return
		}

		// Otherwise, try to serve static file (optional)
		filePath := uiDir + r.URL.Path
		data, err := content.ReadFile(filePath)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Write(data)
	})

	addr := ":8081"

	log.Printf("listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}
