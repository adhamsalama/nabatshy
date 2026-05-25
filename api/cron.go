package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

type CronJob struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Query           string `json:"query"`
	IntervalSeconds int    `json:"interval_seconds"`
	CreatedAt       int64  `json:"created_at"`
}

type CronController struct {
	db      *sql.DB
	tickers map[string]*time.Ticker
	mu      sync.Mutex
}

func NewCronController(db *sql.DB) *CronController {
	c := &CronController{
		db:      db,
		tickers: make(map[string]*time.Ticker),
	}
	rows, err := db.Query(`SELECT id, name, query, interval_seconds, created_at FROM cron_jobs`)
	if err != nil {
		log.Printf("cron: failed to load existing jobs: %v", err)
		return c
	}
	defer rows.Close()
	for rows.Next() {
		var job CronJob
		if err := rows.Scan(&job.ID, &job.Name, &job.Query, &job.IntervalSeconds, &job.CreatedAt); err != nil {
			log.Printf("cron: failed to scan job: %v", err)
			continue
		}
		c.startTicker(job)
	}
	return c
}

func (c *CronController) listCronJobs(w http.ResponseWriter, r *http.Request) {
	rows, err := c.db.QueryContext(r.Context(), `SELECT id, name, query, interval_seconds, created_at FROM cron_jobs`)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to list cron jobs: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	jobs := []CronJob{}
	for rows.Next() {
		var job CronJob
		if err := rows.Scan(&job.ID, &job.Name, &job.Query, &job.IntervalSeconds, &job.CreatedAt); err != nil {
			http.Error(w, fmt.Sprintf("failed to scan cron job: %v", err), http.StatusInternalServerError)
			return
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, fmt.Sprintf("rows error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

func (c *CronController) createCronJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name            string `json:"name"`
		Query           string `json:"query"`
		IntervalSeconds int    `json:"interval_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Query == "" || req.IntervalSeconds <= 0 {
		http.Error(w, "name, query, and interval_seconds (>0) are required", http.StatusBadRequest)
		return
	}

	job := CronJob{
		ID:              fmt.Sprintf("%d", time.Now().UnixNano()),
		Name:            req.Name,
		Query:           req.Query,
		IntervalSeconds: req.IntervalSeconds,
		CreatedAt:       time.Now().UnixNano(),
	}

	_, err := c.db.ExecContext(r.Context(),
		`INSERT INTO cron_jobs (id, name, query, interval_seconds, created_at) VALUES (?, ?, ?, ?, ?)`,
		job.ID, job.Name, job.Query, job.IntervalSeconds, job.CreatedAt,
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to insert cron job: %v", err), http.StatusInternalServerError)
		return
	}

	c.startTicker(job)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(job)
}

func (c *CronController) deleteCronJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	c.mu.Lock()
	if ticker, ok := c.tickers[id]; ok {
		ticker.Stop()
		delete(c.tickers, id)
	}
	c.mu.Unlock()

	_, err := c.db.ExecContext(r.Context(), `DELETE FROM cron_jobs WHERE id = ?`, id)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to delete cron job: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (c *CronController) startTicker(job CronJob) {
	ticker := time.NewTicker(time.Duration(job.IntervalSeconds) * time.Second)

	c.mu.Lock()
	c.tickers[job.ID] = ticker
	c.mu.Unlock()

	go func() {
		for range ticker.C {
			start := time.Now()
			result, err := c.db.Exec(job.Query)
			elapsed := time.Since(start)
			if err != nil {
				log.Printf("[cron] job=%s query=%q error=%v", job.Name, job.Query, err)
				continue
			}
			rows, _ := result.RowsAffected()
			log.Printf("[cron] job=%s rows_affected=%d took=%s query=%q", job.Name, rows, elapsed.Round(time.Millisecond), job.Query)
		}
	}()
}

func (c *CronController) RegisterRoutes(r chi.Router) {
	r.Get("/api/crons", c.listCronJobs)
	r.Post("/api/crons", c.createCronJob)
	r.Delete("/api/crons/{id}", c.deleteCronJob)
}
