package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"nabatshy/db"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/doug-martin/goqu/v9"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type TelemetryCollectorController struct {
	service TelemetryCollectorService
}

func (c *TelemetryCollectorController) ingestTraceHTTPRequest(w http.ResponseWriter, r *http.Request) {
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

func (c *TelemetryCollectorController) formatOldOTELData(
	data []byte,
	req *coltrace.ExportTraceServiceRequest,
) error {
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

func (c *TelemetryCollectorController) RegisterRoutes(r chi.Router) {
	r.Post("/v1/traces", c.ingestTraceHTTPRequest)
}

func InsertResource(
	ch *clickhouse.Conn,
	ctx context.Context, schemaURL string,
) (string, error) {
	resourceID := generateUUID()
	err := (*ch).Exec(ctx, "INSERT INTO resource (resource_id, schema_url) VALUES (?, ?)",
		resourceID, schemaURL)
	return resourceID, err
}

func InsertResourceAttributes(
	ch *clickhouse.Conn,
	ctx context.Context, resourceID string, attrs map[string]string,
) error {
	batch, err := (*ch).PrepareBatch(ctx, "INSERT INTO resource_attributes (resource_id, key, value) VALUES")
	if err != nil {
		return err
	}
	for k, v := range attrs {
		if err := batch.Append(resourceID, k, v); err != nil {
			return err
		}
	}
	return batch.Send()
}

func InsertScope(
	ch *clickhouse.Conn,
	ctx context.Context, name string, resourceID string,
) (string, error) {
	scopeID := generateUUID()
	err := (*ch).Exec(ctx, "INSERT INTO scope (scope_id, name, resource_id) VALUES (?, ?, ?)",
		scopeID, name, resourceID)
	return scopeID, err
}

type Span struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Flags         int32
	Name          string
	StartUnixNano int64
	EndUnixNano   int64
}

func InsertSpans(
	ch *clickhouse.Conn,
	ctx context.Context, scopeID string, spans []Span,
) error {
	batch, err := (*ch).PrepareBatch(ctx, "INSERT INTO span (trace_id, span_id, parent_span_id, flags, name, start_time_unix_nano, end_time_unix_nano, scope_id) VALUES")
	if err != nil {
		return err
	}
	for _, s := range spans {
		if err := batch.Append(s.TraceID, s.SpanID, s.ParentSpanID, s.Flags, s.Name, s.StartUnixNano, s.EndUnixNano, scopeID); err != nil {
			return err
		}
	}
	return batch.Send()
}

type SpanEvent struct {
	SpanID       string
	TimeUnixNano int64
	Name         string
}

func InsertSpanEvents(
	ch *clickhouse.Conn,
	ctx context.Context, events []SpanEvent,
) error {
	batch, err := (*ch).PrepareBatch(ctx, "INSERT INTO event (span_id, time_unix_nano, name) VALUES")
	if err != nil {
		return err
	}
	for _, e := range events {
		if err := batch.Append(e.SpanID, e.TimeUnixNano, e.Name); err != nil {
			return err
		}
	}
	return batch.Send()
}

func generateUUID() string {
	return uuid.New().String()
}

func Run() {
	conn := db.InitClickHouse()
	db := goqu.Dialect("default")
	telService := TelemetryCollectorService{
		Ch: &conn,
		DB: &db,
	}
	telController := TelemetryCollectorController{
		service: telService,
	}

	r := chi.NewRouter()

	telController.RegisterRoutes(r)
	// Start HTTP server
	addr := ":4318"
	log.Printf("listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}
