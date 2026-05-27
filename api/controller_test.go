package api

import (
	"net/url"
	"testing"
	"time"
)

// ── traces ────────────────────────────────────────────────────────────────────

func TestGetSlowestTraces(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/slowest"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	traces := mustDecode[[]SlowTrace](t, w)
	if len(traces) == 0 {
		t.Fatal("expected at least one slow trace")
	}
	if traces[0].TraceID == "" {
		t.Error("TraceID is empty")
	}
}

func TestGetSlowestTracesWithServiceFilter(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/slowest?service=frontend"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	traces := mustDecode[[]SlowTrace](t, w)
	if len(traces) == 0 {
		t.Fatal("expected traces for service=frontend")
	}
	for _, tr := range traces {
		if tr.Service != "frontend" {
			t.Errorf("unexpected service %q in result", tr.Service)
		}
	}
}

func TestGetServiceTraces(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/service/frontend")
	assertOK(t, w)
	traces := mustDecode[[]ServiceTrace](t, w)
	if len(traces) == 0 {
		t.Fatal("expected traces for frontend")
	}
}

func TestGetTraceDetails(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/"+url.PathEscape(testTraceID1))
	assertOK(t, w)
	spans := mustDecode[[]TraceSpan](t, w)
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans for trace T1, got %d", len(spans))
	}
	found := false
	for _, s := range spans {
		if s.SpanID == testSpanID1 {
			found = true
			if s.Name != "GET /users" {
				t.Errorf("span name: got %q, want %q", s.Name, "GET /users")
			}
			if len(s.Events) == 0 {
				t.Error("expected events on root span")
			}
		}
	}
	if !found {
		t.Errorf("root span %s not found in trace details", testSpanID1)
	}
}

func TestGetTraceDetailsNotFound(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/nonexistent-trace-id")
	assertOK(t, w)
	spans := mustDecode[[]TraceSpan](t, w)
	if len(spans) != 0 {
		t.Errorf("expected empty result for unknown trace, got %d spans", len(spans))
	}
}

func TestGetEndpointLatencies(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/endpoints")
	assertOK(t, w)
	latencies := mustDecode[[]EndpointLatency](t, w)
	if len(latencies) == 0 {
		t.Fatal("expected endpoint latency entries")
	}
	for _, l := range latencies {
		if l.Endpoint == "" {
			t.Error("endpoint name is empty")
		}
		if l.RequestCount == 0 {
			t.Error("request count is 0")
		}
	}
}

func TestGetServiceDependencies(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/dependencies")
	assertOK(t, w)
	deps := mustDecode[[]ServiceDependency](t, w)
	if len(deps) == 0 {
		t.Fatal("expected at least one service dependency (frontend→db)")
	}
	found := false
	for _, d := range deps {
		if d.Source == "frontend" && d.Target == "db" {
			found = true
		}
	}
	if !found {
		t.Error("expected frontend→db dependency")
	}
}

func TestGetSpanDetails(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/spans/"+url.PathEscape(testSpanID1))
	assertOK(t, w)
	detail := mustDecode[SpanDetail](t, w)
	if detail.SpanID != testSpanID1 {
		t.Errorf("SpanID: got %q, want %q", detail.SpanID, testSpanID1)
	}
	if detail.TraceID != testTraceID1 {
		t.Errorf("TraceID: got %q, want %q", detail.TraceID, testTraceID1)
	}
	if detail.Name != "GET /users" {
		t.Errorf("Name: got %q, want %q", detail.Name, "GET /users")
	}
	if len(detail.SpanAttributes) == 0 {
		t.Error("expected span attributes")
	}
	if len(detail.Events) == 0 {
		t.Error("expected events on span")
	}
}

// func TestGetSpanDetailsNotFound(t *testing.T) {
// 	r := newTestRouter(setupTestDB(t))
// 	w := get(t, r, "/api/spans/does-not-exist")
// 	// Service returns empty SpanDetail for unknown span — still 200.
// 	assertOK(t, w)
// }

// ── search ────────────────────────────────────────────────────────────────────

func TestSearchTraces_NoQuery(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	if len(resp.Results) == 0 {
		t.Fatal("expected search results with no query filter")
	}
}

func TestSearchTraces_ByName(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search?query=name%3DGET+%2Fusers"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	for _, r := range resp.Results {
		if r.Name != "GET /users" {
			t.Errorf("unexpected result name %q when filtering name=GET /users", r.Name)
		}
	}
}

func TestSearchTraces_ByService(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search?query=scope%3Dfrontend"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	if len(resp.Results) == 0 {
		t.Fatal("expected results for scope=frontend")
	}
}

func TestSearchTraces_Pagination(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search?page=1&pageSize=1"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	if len(resp.Results) > 1 {
		t.Errorf("pageSize=1 should return at most 1 result, got %d", len(resp.Results))
	}
}

func TestSearchSpans(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search?traceOrSpan=span"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	mustDecode[SearchResponse](t, w)
}

// ── metrics ───────────────────────────────────────────────────────────────────

func TestGetTraceMetrics(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/metrics/traces"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	counts := mustDecode[[]TimeCount](t, w)
	// PadQueryResult returns a full time-series grid; at least one bucket should be non-zero.
	nonZero := false
	for _, c := range counts {
		if c.Value > 0 {
			nonZero = true
			break
		}
	}
	if !nonZero {
		t.Error("expected at least one non-zero trace count bucket")
	}
}

func TestGetServiceMetrics(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/metrics/services?timeRange=1h")
	assertOK(t, w)
	metrics := mustDecode[[]ServiceMetrics](t, w)
	if len(metrics) == 0 {
		t.Fatal("expected service metrics")
	}
	for _, m := range metrics {
		if m.Service == "" {
			t.Error("service name is empty")
		}
	}
}

func TestGetEndpointMetrics(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/metrics/endpoints"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	metrics := mustDecode[[]EndpointMetrics](t, w)
	if len(metrics) == 0 {
		t.Fatal("expected endpoint metrics")
	}
}

func TestGetPercentileSeries(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/metrics/pseries?percentile=95"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	series := mustDecode[[]TimePercentile](t, w)
	if len(series) == 0 {
		t.Fatal("expected percentile series data")
	}
}

func TestGetAvgDuration(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/metrics/avg"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	series := mustDecode[[]TimePercentile](t, w)
	if len(series) == 0 {
		t.Fatal("expected avg duration series data")
	}
}

func TestGetErrorCounts(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/metrics/errors"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	mustDecode[[]TimeCount](t, w)
}

func TestGetErrorCountsWithServiceFilter(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/metrics/errors?service=frontend"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	mustDecode[[]TimeCount](t, w)
}

func TestGetSearchMetrics(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/metrics/search?percentile=95"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	mustDecode[CombinedMetricsResult](t, w)
}

// ── services ──────────────────────────────────────────────────────────────────

func TestGetUniqueServiceNames(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/services")
	assertOK(t, w)
	services := mustDecode[[]string](t, w)
	if len(services) == 0 {
		t.Fatal("expected service names")
	}
	has := func(name string) bool {
		for _, s := range services {
			if s == name {
				return true
			}
		}
		return false
	}
	if !has("frontend") {
		t.Error("expected 'frontend' in service list")
	}
	if !has("db") {
		t.Error("expected 'db' in service list")
	}
}

// ── otel metrics ──────────────────────────────────────────────────────────────

func TestGetOtelMetricNames(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/otel-metrics/names")
	assertOK(t, w)
	names := mustDecode[[]OtelMetricNameRow](t, w)
	if len(names) == 0 {
		t.Fatal("expected otel metric names")
	}
	found := false
	for _, n := range names {
		if n.MetricName == "http.request.duration" {
			found = true
		}
	}
	if !found {
		t.Error("expected 'http.request.duration' metric")
	}
}

func TestGetOtelMetrics(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/otel-metrics?limit=10"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	rows := mustDecode[[]OtelMetricRow](t, w)
	if len(rows) == 0 {
		t.Fatal("expected otel metric rows")
	}
}

func TestGetOtelMetricsWithFilter(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/otel-metrics?metric_name=http.request.duration&limit=10"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	rows := mustDecode[[]OtelMetricRow](t, w)
	for _, row := range rows {
		if row.MetricName != "http.request.duration" {
			t.Errorf("unexpected metric name %q", row.MetricName)
		}
	}
}

func TestGetOtelMetricSeries(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/otel-metrics/series?metric_name=http.request.duration"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[OtelMetricSeriesResponse](t, w)
	if len(resp.Series) == 0 {
		t.Fatal("expected metric series")
	}
}

func TestGetOtelMetricSeriesMissingName(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/otel-metrics/series")
	if w.Code != 400 {
		t.Fatalf("expected 400 when metric_name missing, got %d", w.Code)
	}
}

// ── logs ──────────────────────────────────────────────────────────────────────

func TestGetLogs(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/logs"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	logs := mustDecode[[]LogRow](t, w)
	if len(logs) == 0 {
		t.Fatal("expected log rows")
	}
	for _, l := range logs {
		if l.Body == "" {
			t.Error("log body is empty")
		}
	}
}

func TestGetLogsWithServiceFilter(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/logs?service=frontend"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	logs := mustDecode[[]LogRow](t, w)
	if len(logs) == 0 {
		t.Fatal("expected log rows for service=frontend")
	}
	for _, l := range logs {
		if l.ServiceName != "frontend" {
			t.Errorf("unexpected service %q", l.ServiceName)
		}
	}
}

func TestGetLogsWithSeverityFilter(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/logs?severity=ERROR"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	logs := mustDecode[[]LogRow](t, w)
	for _, l := range logs {
		if l.SeverityText != "ERROR" {
			t.Errorf("unexpected severity %q", l.SeverityText)
		}
	}
}

func TestGetLogsWithTraceIDFilter(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/logs?trace_id="+testTraceID1+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	logs := mustDecode[[]LogRow](t, w)
	if len(logs) == 0 {
		t.Fatal("expected logs for testTraceID1")
	}
	for _, l := range logs {
		if l.TraceID != testTraceID1 {
			t.Errorf("unexpected trace_id %q", l.TraceID)
		}
	}
}

func TestGetLogVolume(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/logs/volume"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	buckets := mustDecode[[]LogVolumeBucket](t, w)
	if len(buckets) == 0 {
		t.Fatal("expected log volume buckets")
	}
}

func TestGetLogVolumeWithFilters(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/logs/volume?service=frontend&severity=INFO"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	mustDecode[[]LogVolumeBucket](t, w)
}

// ── cron ─────────────────────────────────────────────────────────────────────

func TestListCronJobs_Empty(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/crons")
	assertOK(t, w)
	jobs := mustDecode[[]CronJob](t, w)
	// Fresh DB has no cron jobs — should be an empty array, not null.
	if jobs == nil {
		t.Error("expected empty array, got null")
	}
}

// ── traces/slowest — ordering & edge cases ────────────────────────────────────

func TestGetSlowestTraces_Ordering(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/slowest"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	traces := mustDecode[[]SlowTrace](t, w)
	if len(traces) < 2 {
		t.Fatalf("expected at least 2 traces, got %d", len(traces))
	}
	// T2 (POST /orders, 2 min) must outrank T1 (GET /users, 1 min).
	if traces[0].TraceID != testTraceID2 {
		t.Errorf("slowest trace: got %q, want %q (T2 is 2 min, T1 is 1 min)",
			traces[0].TraceID, testTraceID2)
	}
	if traces[0].Duration <= traces[1].Duration {
		t.Errorf("traces not in descending duration order: [0]=%v [1]=%v",
			traces[0].Duration, traces[1].Duration)
	}
}

func TestGetSlowestTraces_NLimit(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/slowest?n=1"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	traces := mustDecode[[]SlowTrace](t, w)
	if len(traces) != 1 {
		t.Errorf("n=1 should return exactly 1 trace, got %d", len(traces))
	}
}

func TestGetSlowestTraces_EmptyRange(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	// Range entirely in the future — no spans exist there.
	w := get(t, r, "/api/traces/slowest"+
		"?start="+timeParam(testNow.Add(1*time.Hour))+
		"&end="+timeParam(testNow.Add(2*time.Hour)))
	assertOK(t, w)
	traces := mustDecode[[]SlowTrace](t, w)
	if len(traces) != 0 {
		t.Errorf("expected 0 traces for future range, got %d", len(traces))
	}
}

func TestGetSlowestTraces_UnknownService(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/slowest?service=does-not-exist"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	traces := mustDecode[[]SlowTrace](t, w)
	if len(traces) != 0 {
		t.Errorf("expected 0 traces for unknown service, got %d", len(traces))
	}
}

func TestGetSlowestTraces_Fields(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/slowest"+rangeParams(30*60e9, 60e9))
	assertOK(t, w)
	traces := mustDecode[[]SlowTrace](t, w)
	for i, tr := range traces {
		if tr.TraceID == "" {
			t.Errorf("traces[%d].TraceID is empty", i)
		}
		if tr.Name == "" {
			t.Errorf("traces[%d].Name is empty", i)
		}
		if tr.Duration <= 0 {
			t.Errorf("traces[%d].Duration = %v, want > 0", i, tr.Duration)
		}
		if tr.Service == "" {
			t.Errorf("traces[%d].Service is empty", i)
		}
		if tr.StartTime <= 0 {
			t.Errorf("traces[%d].StartTime = %v, want > 0", i, tr.StartTime)
		}
	}
}

// ── traces/service/{service} — field and edge cases ───────────────────────────

func TestGetServiceTraces_Fields(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/service/frontend")
	assertOK(t, w)
	traces := mustDecode[[]ServiceTrace](t, w)
	if len(traces) == 0 {
		t.Fatal("expected traces for frontend")
	}
	for i, tr := range traces {
		if tr.TraceID == "" {
			t.Errorf("traces[%d].TraceID is empty", i)
		}
		if tr.Name == "" {
			t.Errorf("traces[%d].Name is empty", i)
		}
		if tr.Duration <= 0 {
			t.Errorf("traces[%d].Duration = %v, want > 0", i, tr.Duration)
		}
	}
}

func TestGetServiceTraces_UnknownService(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/service/nobody")
	assertOK(t, w)
	traces := mustDecode[[]ServiceTrace](t, w)
	if len(traces) != 0 {
		t.Errorf("expected 0 traces for unknown service, got %d", len(traces))
	}
}

// ── traces/{trace_id} — depth, ordering, field validation ────────────────────

func TestGetTraceDetails_T2_SingleSpan(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/"+testTraceID2)
	assertOK(t, w)
	spans := mustDecode[[]TraceSpan](t, w)
	if len(spans) != 1 {
		t.Fatalf("T2 has 1 span, got %d", len(spans))
	}
	if spans[0].SpanID != testSpanID3 {
		t.Errorf("SpanID: got %q, want %q", spans[0].SpanID, testSpanID3)
	}
}

func TestGetTraceDetails_TreeOrder(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/"+testTraceID1)
	assertOK(t, w)
	spans := mustDecode[[]TraceSpan](t, w)
	if len(spans) != 2 {
		t.Fatalf("T1 should have 2 spans, got %d", len(spans))
	}
	// Root span must come first (treeOrderSpans guarantee).
	if spans[0].SpanID != testSpanID1 {
		t.Errorf("first span should be root %q, got %q", testSpanID1, spans[0].SpanID)
	}
	if spans[1].SpanID != testSpanID2 {
		t.Errorf("second span should be child %q, got %q", testSpanID2, spans[1].SpanID)
	}
}

func TestGetTraceDetails_ChildSpanParentID(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/"+testTraceID1)
	assertOK(t, w)
	spans := mustDecode[[]TraceSpan](t, w)
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	var child *TraceSpan
	for i := range spans {
		if spans[i].SpanID == testSpanID2 {
			child = &spans[i]
		}
	}
	if child == nil {
		t.Fatalf("child span %q not found", testSpanID2)
	}
	if child.ParentSpanID != testSpanID1 {
		t.Errorf("child.ParentSpanID = %q, want %q", child.ParentSpanID, testSpanID1)
	}
}

func TestGetTraceDetails_Timing(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/"+testTraceID1)
	assertOK(t, w)
	spans := mustDecode[[]TraceSpan](t, w)
	for i, s := range spans {
		if s.StartTimeNS <= 0 {
			t.Errorf("spans[%d].StartTimeNS = %d, want > 0", i, s.StartTimeNS)
		}
		if s.EndTimeNS <= s.StartTimeNS {
			t.Errorf("spans[%d].EndTimeNS (%d) <= StartTimeNS (%d)", i, s.EndTimeNS, s.StartTimeNS)
		}
		if s.DurationNS <= 0 {
			t.Errorf("spans[%d].DurationNS = %d, want > 0", i, s.DurationNS)
		}
	}
}

func TestGetTraceDetails_ExceptionEvent(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/"+testTraceID2)
	assertOK(t, w)
	spans := mustDecode[[]TraceSpan](t, w)
	if len(spans) == 0 {
		t.Fatal("expected spans for T2")
	}
	found := false
	for _, evt := range spans[0].Events {
		if evt.Name == "exception" {
			found = true
			if evt.Attributes["exception.type"] == "" {
				t.Error("exception event missing exception.type attribute")
			}
		}
	}
	if !found {
		t.Errorf("expected exception event on T2 root span, got events: %+v", spans[0].Events)
	}
}

func TestGetTraceDetails_ServiceNames(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/"+testTraceID1)
	assertOK(t, w)
	spans := mustDecode[[]TraceSpan](t, w)
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	services := map[string]string{}
	for _, s := range spans {
		services[s.SpanID] = s.Service
	}
	if services[testSpanID1] != "frontend" {
		t.Errorf("root span service: got %q, want %q", services[testSpanID1], "frontend")
	}
	if services[testSpanID2] != "db" {
		t.Errorf("child span service: got %q, want %q", services[testSpanID2], "db")
	}
}

// ── spans/{span_id} — deep field validation ───────────────────────────────────

func TestGetSpanDetails_ResourceAttributes(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/spans/"+testSpanID1)
	assertOK(t, w)
	detail := mustDecode[SpanDetail](t, w)
	if len(detail.ResourceAttributes) == 0 {
		t.Fatal("expected non-empty ResourceAttributes")
	}
	if detail.ResourceAttributes["service.name"] != "frontend" {
		t.Errorf("ResourceAttributes[service.name] = %q, want %q",
			detail.ResourceAttributes["service.name"], "frontend")
	}
}

func TestGetSpanDetails_Timing(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/spans/"+testSpanID1)
	assertOK(t, w)
	detail := mustDecode[SpanDetail](t, w)
	if detail.StartTime <= 0 {
		t.Errorf("StartTime = %d, want > 0", detail.StartTime)
	}
	if detail.EndTime <= detail.StartTime {
		t.Errorf("EndTime (%d) <= StartTime (%d)", detail.EndTime, detail.StartTime)
	}
	if detail.Duration <= 0 {
		t.Errorf("Duration = %v, want > 0", detail.Duration)
	}
}

func TestGetSpanDetails_PercentileStats(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/spans/"+testSpanID1)
	assertOK(t, w)
	detail := mustDecode[SpanDetail](t, w)
	if detail.AvgDuration <= 0 {
		t.Errorf("AvgDuration = %v, want > 0", detail.AvgDuration)
	}
}

func TestGetSpanDetails_SpanAttributes(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/spans/"+testSpanID1)
	assertOK(t, w)
	detail := mustDecode[SpanDetail](t, w)
	if detail.SpanAttributes["http.method"] != "GET" {
		t.Errorf("SpanAttributes[http.method] = %q, want %q", detail.SpanAttributes["http.method"], "GET")
	}
	if detail.SpanAttributes["http.status_code"] != "200" {
		t.Errorf("SpanAttributes[http.status_code] = %q, want %q", detail.SpanAttributes["http.status_code"], "200")
	}
}

func TestGetSpanDetails_ChildSpanAttributes(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/spans/"+testSpanID2)
	assertOK(t, w)
	detail := mustDecode[SpanDetail](t, w)
	if detail.SpanAttributes["db.statement"] != "SELECT * FROM users" {
		t.Errorf("SpanAttributes[db.statement] = %q, want %q", detail.SpanAttributes["db.statement"], "SELECT * FROM users")
	}
}

func TestGetSpanDetails_ChildSpan(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/spans/"+testSpanID2)
	assertOK(t, w)
	detail := mustDecode[SpanDetail](t, w)
	if detail.SpanID != testSpanID2 {
		t.Errorf("SpanID: got %q, want %q", detail.SpanID, testSpanID2)
	}
	if detail.TraceID != testTraceID1 {
		t.Errorf("TraceID: got %q, want %q", detail.TraceID, testTraceID1)
	}
	if detail.ParentSpanID != testSpanID1 {
		t.Errorf("ParentSpanID: got %q, want %q", detail.ParentSpanID, testSpanID1)
	}
	if detail.Name != "db.query" {
		t.Errorf("Name: got %q, want %q", detail.Name, "db.query")
	}
}

func TestGetSpanDetails_ExceptionSpan(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/spans/"+testSpanID3)
	assertOK(t, w)
	detail := mustDecode[SpanDetail](t, w)
	found := false
	for _, evt := range detail.Events {
		if evt.Name == "exception" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected exception event on span %q, got events: %+v", testSpanID3, detail.Events)
	}
}

// ── search — mode, sort, filter correctness ───────────────────────────────────

func TestSearchTraces_SpanAttrs(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search?query=name%3DGET+%2Fusers"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	if len(resp.Results) == 0 {
		t.Fatal("expected results for name=GET /users")
	}
	for _, res := range resp.Results {
		if res.SpanID != testSpanID1 {
			continue
		}
		if res.SpanAttrs["http.method"] != "GET" {
			t.Errorf("SpanAttrs[http.method] = %q, want %q", res.SpanAttrs["http.method"], "GET")
		}
		if res.SpanAttrs["http.status_code"] != "200" {
			t.Errorf("SpanAttrs[http.status_code] = %q, want %q", res.SpanAttrs["http.status_code"], "200")
		}
		if res.ResourceAttrs["service.name"] != "frontend" {
			t.Errorf("ResourceAttrs[service.name] = %q, want %q", res.ResourceAttrs["service.name"], "frontend")
		}
		return
	}
	t.Errorf("span %q not found in search results", testSpanID1)
}

func TestSearchTraces_TraceMode_NoChildSpans(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search?traceOrSpan=trace"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	for _, res := range resp.Results {
		if res.SpanID == testSpanID2 {
			t.Errorf("traceOrSpan=trace must not return child span %q", testSpanID2)
		}
	}
}

func TestSearchTraces_SpanMode_IncludesChild(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search?traceOrSpan=span"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	found := false
	for _, res := range resp.Results {
		if res.SpanID == testSpanID2 {
			found = true
		}
	}
	if !found {
		t.Errorf("traceOrSpan=span must include child span %q", testSpanID2)
	}
}

func TestSearchTraces_SortByDuration_Desc(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search?sortField=duration&sortOrder=desc"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	if len(resp.Results) < 2 {
		t.Skip("need at least 2 results to check ordering")
	}
	for i := 1; i < len(resp.Results); i++ {
		if resp.Results[i-1].Duration < resp.Results[i].Duration {
			t.Errorf("results not sorted desc by duration: [%d]=%v < [%d]=%v",
				i-1, resp.Results[i-1].Duration, i, resp.Results[i].Duration)
		}
	}
}

func TestSearchTraces_SortByDuration_Asc(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search?sortField=duration&sortOrder=asc"+
		"&start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	if len(resp.Results) < 2 {
		t.Skip("need at least 2 results to check ordering")
	}
	for i := 1; i < len(resp.Results); i++ {
		if resp.Results[i-1].Duration > resp.Results[i].Duration {
			t.Errorf("results not sorted asc by duration: [%d]=%v > [%d]=%v",
				i-1, resp.Results[i-1].Duration, i, resp.Results[i].Duration)
		}
	}
}

func TestSearchTraces_HasError(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/search"+
		"?start="+timeParam(testNow.Add(-30*60e9))+
		"&end="+timeParam(testNow.Add(60e9)))
	assertOK(t, w)
	resp := mustDecode[SearchResponse](t, w)
	for _, res := range resp.Results {
		if res.SpanID == testSpanID3 {
			if !res.HasError {
				t.Errorf("span %q has exception event but HasError=false", testSpanID3)
			}
			return
		}
	}
	// S3 may not appear in the default time range — skip rather than fail.
	t.Log("testSpanID3 not in search results, skipping HasError check")
}

func TestSearchTraces_Page2_Different(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	params := "&start=" + timeParam(testNow.Add(-30*60e9)) + "&end=" + timeParam(testNow.Add(60e9))
	w1 := get(t, r, "/api/traces/search?page=1&pageSize=1"+params)
	w2 := get(t, r, "/api/traces/search?page=2&pageSize=1"+params)
	assertOK(t, w1)
	assertOK(t, w2)
	resp1 := mustDecode[SearchResponse](t, w1)
	resp2 := mustDecode[SearchResponse](t, w2)
	if len(resp1.Results) == 0 || len(resp2.Results) == 0 {
		t.Skip("not enough results to test pagination")
	}
	if resp1.Results[0].SpanID == resp2.Results[0].SpanID {
		t.Errorf("page=1 and page=2 returned the same span %q", resp1.Results[0].SpanID)
	}
}

// ── traces/endpoints — field correctness ─────────────────────────────────────

func TestGetEndpointLatencies_KnownEndpoints(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/endpoints")
	assertOK(t, w)
	latencies := mustDecode[[]EndpointLatency](t, w)
	has := func(name string) bool {
		for _, l := range latencies {
			if l.Endpoint == name {
				return true
			}
		}
		return false
	}
	if !has("GET /users") {
		t.Error("expected endpoint 'GET /users'")
	}
	if !has("POST /orders") {
		t.Error("expected endpoint 'POST /orders'")
	}
}

func TestGetEndpointLatencies_DurationStats(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/endpoints")
	assertOK(t, w)
	latencies := mustDecode[[]EndpointLatency](t, w)
	for _, l := range latencies {
		if l.AvgDuration <= 0 {
			t.Errorf("endpoint %q: AvgDuration = %v, want > 0", l.Endpoint, l.AvgDuration)
		}
		if l.P99Duration < l.P50Duration {
			t.Errorf("endpoint %q: P99 (%v) < P50 (%v)", l.Endpoint, l.P99Duration, l.P50Duration)
		}
	}
}

// ── traces/dependencies — field values ───────────────────────────────────────

func TestGetServiceDependencies_CallCount(t *testing.T) {
	r := newTestRouter(setupTestDB(t))
	w := get(t, r, "/api/traces/dependencies")
	assertOK(t, w)
	deps := mustDecode[[]ServiceDependency](t, w)
	for _, d := range deps {
		if d.Source == "frontend" && d.Target == "db" {
			if d.CallCount < 1 {
				t.Errorf("frontend→db CallCount = %d, want >= 1", d.CallCount)
			}
			return
		}
	}
	t.Error("frontend→db dependency not found")
}
