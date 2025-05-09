package receiver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type TelemetryController struct {
	service TelemetryService
}

func (c *TelemetryController) ingestTraceHTTPRequest(w http.ResponseWriter, r *http.Request) {
	fmt.Println("ingesting...")
	if r.Method != http.MethodPost {
		fmt.Println("only POST allowed")
		http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var req coltrace.ExportTraceServiceRequest
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Println("failed to read body: ")
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	contentType := r.Header.Get("Content-Type")

	switch contentType {
	case "application/x-protobuf":
		{
			if protoErr := proto.Unmarshal(body, &req); protoErr != nil {
				fmt.Println("proto err", protoErr)
				http.Error(w, "invalid protobuf: "+protoErr.Error(), http.StatusBadRequest)
				return
			}
		}
	case "application/json":
		{

			if protoErr := protojson.Unmarshal(body, &req); protoErr != nil {
				fmt.Println("Cannot marshal json data. Will try the old OTEL format...")
				// try to handle the old format (instrumentationLibrary)
				oldFormatErr := c.formatOldOTELData(body, &req)
				if oldFormatErr != nil {
					fmt.Println("json err", protoErr)
					http.Error(w, "invalid json: "+protoErr.Error(), http.StatusBadRequest)
					return
				}
			}

			fmt.Printf("ingesting trace: %v\n", req)

		}
	default:
		{
			fmt.Printf("unsupported content-type: %v\n", contentType)
			http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
			return
		}
	}

	ingestionErr := c.service.ingestTrace(&req)
	if ingestionErr != nil {
		errMsg := fmt.Sprintf("ingestion err: %v\n", ingestionErr)
		fmt.Println(errMsg)
		panic(errMsg)
	}
	// Send empty success response
	resp := &coltrace.ExportTraceServiceResponse{}
	out, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	w.Write(out)
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

func (c *TelemetryController) formatOldOTELData(
	data []byte,
	req *coltrace.ExportTraceServiceRequest,
) error {
	fmt.Println("formatting old otel data\n*************************************************************\n")
	var top map[string]any
	if err := json.Unmarshal(data, &top); err != nil {
		return err
	}

	// Helper function to normalize values
	normalizeAttributeValue := func(val map[string]any) any {
		if inner, ok := val["Value"].(map[string]any); ok {
			if stringVal, ok := inner["StringValue"].(string); ok {
				return stringVal
			}
			if intVal, ok := inner["IntValue"].(float64); ok {
				return intVal
			}
			if boolVal, ok := inner["BoolValue"].(bool); ok {
				return boolVal
			}
			if doubleVal, ok := inner["DoubleValue"].(float64); ok {
				return doubleVal
			}
		}
		return val
	}

	// Process resourceSpans
	if rsList, ok := top["resourceSpans"].([]any); ok {
		for _, rsItem := range rsList {
			rsMap, ok := rsItem.(map[string]any)
			if !ok {
				continue
			}

			// Rename instrumentationLibrarySpans -> scopeSpans
			if old, found := rsMap["instrumentationLibrarySpans"]; found {
				rsMap["scopeSpans"] = old
				delete(rsMap, "instrumentationLibrarySpans")
			}

			// Extract service.name from resource.attributes
			var serviceName string
			if resourceMap, ok := rsMap["resource"].(map[string]any); ok {
				if attrs, ok := resourceMap["attributes"].([]any); ok {
					for _, attr := range attrs {
						attrMap, ok := attr.(map[string]any)
						if !ok {
							continue
						}
						if key, _ := attrMap["key"].(string); key == "service.name" {
							if val, ok := attrMap["value"].(map[string]any); ok {
								if normalized := normalizeAttributeValue(val); normalized != nil {
									if s, ok := normalized.(string); ok {
										serviceName = s
									}
								}
							}
						}
					}
				}
			}

			// Process scopeSpans
			if ssList, ok := rsMap["scopeSpans"].([]any); ok {
				for _, ssItem := range ssList {
					ssMap, ok := ssItem.(map[string]any)
					if !ok {
						continue
					}
					if _, hasScope := ssMap["scope"]; !hasScope {
						ssMap["scope"] = map[string]any{}
					}

					// Process spans
					if spans, ok := ssMap["spans"].([]any); ok {
						for _, spanItem := range spans {
							spanMap, ok := spanItem.(map[string]any)
							if !ok {
								continue
							}

							// Inject serviceName into each span
							if serviceName != "" {
								spanMap["serviceName"] = serviceName
							}

							// Normalize attributes
							if attrs, ok := spanMap["attributes"].([]any); ok {
								normalizedAttrs := make([]any, 0, len(attrs))
								for _, attr := range attrs {
									attrMap, ok := attr.(map[string]any)
									if !ok {
										continue
									}
									if val, ok := attrMap["value"].(map[string]any); ok {
										attrMap["value"] = normalizeAttributeValue(val)
									}
									normalizedAttrs = append(normalizedAttrs, attrMap)
								}

								// Merge normalized attributes into resource.attributes
								resourceMap, ok := rsMap["resource"].(map[string]any)
								if !ok {
									resourceMap = map[string]any{}
									rsMap["resource"] = resourceMap
								}
								resAttrs, _ := resourceMap["attributes"].([]any)
								resourceMap["attributes"] = append(resAttrs, normalizedAttrs...)
							}
						}
					}
				}
			}
		}
	}

	// Re-marshal the normalized structure and populate the request
	normalized, err := json.Marshal(top)
	if err != nil {
		return err
	}

	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	return opts.Unmarshal(normalized, req)
}

func (c *TelemetryController) searchTraces(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")

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
		dateRange = getDateRangeFromQuery(timeRange)
	}
	results, err := c.service.SearchTraces(r.Context(), dateRange, query, page, pageSize, sort)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to search traces: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (c *TelemetryController) getTraceMetrics(w http.ResponseWriter, r *http.Request) {
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
		dateRange = getDateRangeFromQuery(timeRange)
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

type DateRange struct {
	Start time.Time
	End   time.Time
}

func getDateRangeFromQuery(timeRange string) DateRange {
	end := time.Now()
	if len(timeRange) < 2 {
		return DateRange{Start: end, End: end} // invalid input fallback
	}

	unit := timeRange[len(timeRange)-1:]
	valueStr := timeRange[:len(timeRange)-1]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return DateRange{Start: end, End: end} // invalid number
	}

	var duration time.Duration
	switch unit {
	case "s":
		duration = time.Duration(value) * time.Second
	case "m":
		duration = time.Duration(value) * time.Minute
	case "h":
		duration = time.Duration(value) * time.Hour
	case "d":
		duration = time.Duration(value) * 24 * time.Hour
	default:
		return DateRange{Start: end, End: end} // unsupported unit
	}

	start := end.Add(-duration)
	dateRange := DateRange{Start: start, End: end}

	fmt.Printf("dateRange: %v\n", dateRange)
	return dateRange
}

func (c *TelemetryController) getEndpointMetrics(w http.ResponseWriter, r *http.Request) {
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
		dateRange = getDateRangeFromQuery(timeRange)
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
	buckets := 10
	if bs := q.Get("buckets"); bs != "" {
		if v, err := strconv.Atoi(bs); err == nil {
			buckets = v
		}
	}

	var dr DateRange
	if startStr, endStr := q.Get("start"), q.Get("end"); startStr != "" && endStr != "" {
		startT, err1 := time.Parse(time.RFC3339, startStr)
		endT, err2 := time.Parse(time.RFC3339, endStr)
		if err1 != nil || err2 != nil {
			http.Error(w, "invalid start or end timestamp", http.StatusBadRequest)
			return
		}
		dr = DateRange{Start: startT, End: endT}
	} else {
		dr = getDateRangeFromQuery(q.Get("timeRange"))
	}

	series, err := c.service.GetPercentileSeries(r.Context(), dr, pct, buckets)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get p%d series: %v", pct, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(series)
}

func (c *TelemetryController) getAvgDuration(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var dr DateRange
	if startStr, endStr := q.Get("start"), q.Get("end"); startStr != "" && endStr != "" {
		startT, err1 := time.Parse(time.RFC3339, startStr)
		endT, err2 := time.Parse(time.RFC3339, endStr)
		if err1 != nil || err2 != nil {
			http.Error(w, "invalid start or end timestamp", http.StatusBadRequest)
			return
		}
		dr = DateRange{Start: startT, End: endT}
	} else {
		dr = getDateRangeFromQuery(q.Get("timeRange"))
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
	r.Post("/v1/traces", c.ingestTraceHTTPRequest)
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
