package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"nabatshy/utils"

	"github.com/go-chi/chi/v5"
)

type (
	DateRange      = utils.DateRange
	TimePercentile = utils.TimePercentile
)

var (
	ParseDateRange        = utils.ParseDateRange
	GetDateRangeFromQuery = utils.GetDateRangeFromQuery
)

type TelemetryController struct {
	service TelemetryService
}

func (c *TelemetryController) getTopNSlowestTraces(w http.ResponseWriter, r *http.Request) {
	nParam := r.URL.Query().Get("n")
	if nParam == "" {
		nParam = "10"
	}
	n64, err := strconv.ParseUint(nParam, 10, 32)
	if err != nil {
		http.Error(w, "invalid parameter 'n'", http.StatusBadRequest)
		return
	}
	n := uint(n64)

	// Fetch data
	traces, err := c.service.GetTopSlowTraces(r.Context(), n)
	if err != nil {
		http.Error(w, "failed to fetch traces: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Write JSON response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(traces); err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (c *TelemetryController) getServiceTraces(w http.ResponseWriter, r *http.Request) {
	service := chi.URLParam(r, "service")

	traces, err := c.service.GetServiceTraces(r.Context(), service)
	if err != nil {
		http.Error(w, "failed to fetch traces: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(traces); err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (c *TelemetryController) getTraceDetails(w http.ResponseWriter, r *http.Request) {
	traceID := chi.URLParam(r, "trace_id")
	traceID, err := url.QueryUnescape(traceID)
	if err != nil {
		http.Error(w, "invalid trace_id", http.StatusBadRequest)
		return
	}

	spans, err := c.service.GetTraceDetails(r.Context(), traceID)
	if err != nil {
		http.Error(w, "failed to fetch trace details: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(spans); err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (c *TelemetryController) getEndpointLatencies(w http.ResponseWriter, r *http.Request) {
	latencies, err := c.service.GetEndpointLatencies(r.Context())
	if err != nil {
		http.Error(w, "failed to fetch endpoint latencies: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(latencies); err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (c *TelemetryController) getServiceDependencies(w http.ResponseWriter, r *http.Request) {
	dependencies, err := c.service.GetServiceDependencies(r.Context())
	if err != nil {
		http.Error(w, "failed to fetch service dependencies: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(dependencies); err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (c *TelemetryController) getTraceHeatmap(w http.ResponseWriter, r *http.Request) {
	heatmap, err := c.service.GetTraceHeatmap(r.Context())
	if err != nil {
		http.Error(w, "failed to fetch trace heatmap: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(heatmap); err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (c *TelemetryController) getSpanDetails(w http.ResponseWriter, r *http.Request) {
	spanID := chi.URLParam(r, "span_id")
	spanID, err := url.QueryUnescape(spanID)
	if err != nil {
		http.Error(w, "invalid span_id", http.StatusBadRequest)
		return
	}
	detail, err := c.service.GetSpanDetails(r.Context(), spanID)
	if err != nil {
		http.Error(w, "failed to fetch span details: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(detail); err != nil {
		http.Error(w, "failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (c *TelemetryController) searchTraces(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	percentileStr := r.URL.Query().Get("percentile")
	percentile := 95
	if percentileStr != "" {
		p, err := strconv.Atoi(percentileStr)
		if err == nil {
			percentile = p
		}
	}
	page, err := strconv.Atoi(r.URL.Query().Get("page"))
	if err != nil || page < 1 {
		page = 1
	}

	pageSize, err := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if err != nil || pageSize < 1 {
		pageSize = 10
	}

	sortField := r.URL.Query().Get("sortField")

	sortOrder := r.URL.Query().Get("sortOrder")
	if sortOrder != "asc" && sortOrder != "desc" {
		sortOrder = "desc" // default to descending
	}

	sort := SortOption{
		Field: sortField,
		Order: sortOrder,
	}
	var dateRange DateRange
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	if startStr != "" && endStr != "" {
		startTime, err1 := time.Parse(time.RFC3339, startStr)
		endTime, err2 := time.Parse(time.RFC3339, endStr)
		if err1 == nil && err2 == nil {
			dateRange = DateRange{Start: startTime, End: endTime}
		} else {
			http.Error(w, "invalid start or end time format", http.StatusBadRequest)
			return
		}
	} else {
		timeRange := r.URL.Query().Get("timeRange")
		dateRange = GetDateRangeFromQuery(timeRange)
	}
	results, err := c.service.SearchTraces(r.Context(), dateRange, query, page, pageSize, sort, percentile)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to search traces: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (c *TelemetryController) getTraceMetrics(w http.ResponseWriter, r *http.Request) {
	dateRange, err := ParseDateRange(r.URL.Query(), "start", "end", "timeRange")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	metrics, err := c.service.GetTraceCounts(r.Context(), dateRange)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get trace metrics: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (c *TelemetryController) getServiceMetrics(w http.ResponseWriter, r *http.Request) {
	timeRange := r.URL.Query().Get("timeRange")
	if timeRange == "" {
		timeRange = "24h" // Default to last 24 hours
	}

	// Parse custom date range if provided
	var startTime, endTime *time.Time
	if start := r.URL.Query().Get("start"); start != "" {
		if t, err := time.Parse(time.RFC3339, start); err == nil {
			startTime = &t
		}
	}
	if end := r.URL.Query().Get("end"); end != "" {
		if t, err := time.Parse(time.RFC3339, end); err == nil {
			endTime = &t
		}
	}

	metrics, err := c.service.GetServiceMetrics(r.Context(), timeRange, startTime, endTime)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get service metrics: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (c *TelemetryController) getEndpointMetrics(w http.ResponseWriter, r *http.Request) {
	dateRange, err := ParseDateRange(r.URL.Query(), "start", "end", "timeRange")
	if err != nil {
		http.Error(w, "invalid date range", http.StatusBadRequest)
		return
	}

	metrics, err := c.service.GetEndpointMetrics(r.Context(), dateRange)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get endpoint metrics: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (c *TelemetryController) getPMetrics(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pct := 95
	if ps := q.Get("percentile"); ps != "" {
		if v, err := strconv.Atoi(ps); err == nil {
			pct = v
		}
	}

	dr, err := ParseDateRange(q, "start", "end", "timeRange")
	if err != nil {
		http.Error(w, "invalid date range", http.StatusBadRequest)
		return
	}

	series, err := c.service.GetPercentileSeries(r.Context(), dr, pct)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get p%d series: %v", pct, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(series)
}

func (c *TelemetryController) getAvgDuration(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dr, err := ParseDateRange(q, "start", "end", "timeRange")
	if err != nil {
		http.Error(w, "invalid date range", http.StatusBadRequest)
		return
	}

	series, err := c.service.GetAvgDuration(r.Context(), dr)
	if err != nil {
		http.Error(w, "failed to get avg", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(series)
}

func (c *TelemetryController) RegisterRoutes(r chi.Router) {
	r.Get("/v1/traces/slowest", c.getTopNSlowestTraces)
	r.Get("/v1/traces/service/{service}", c.getServiceTraces)
	r.Get("/v1/traces/{trace_id}", c.getTraceDetails)
	r.Get("/v1/traces/endpoints", c.getEndpointLatencies)
	r.Get("/v1/traces/dependencies", c.getServiceDependencies)
	r.Get("/v1/traces/heatmap", c.getTraceHeatmap)
	r.Get("/v1/spans/{span_id}", c.getSpanDetails)
	r.Get("/v1/search", c.searchTraces)

	r.Get("/api/metrics/traces", c.getTraceMetrics)
	r.Get("/api/metrics/services", c.getServiceMetrics)
	r.Get("/api/metrics/endpoints", c.getEndpointMetrics)
	r.Get("/api/metrics/pseries", c.getPMetrics)
	r.Get("/api/metrics/avg", c.getAvgDuration)
}
