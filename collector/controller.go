package collector

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"nabatshy/utils"

	"github.com/doug-martin/goqu/v9"
	"github.com/go-chi/chi/v5"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Span = utils.Span

type TelemetryCollectorController struct {
	service TelemetryCollectorService
}

func (c *TelemetryCollectorController) ingestTraceHTTPRequest(w http.ResponseWriter, r *http.Request) {
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
				oldFormatErr := c.formatOldOTELData(body, &req)
				if oldFormatErr != nil {
					fmt.Println("json err", protoErr)
					http.Error(w, "invalid json: "+protoErr.Error(), http.StatusBadRequest)
					return
				}
			}
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

	if rsList, ok := top["resourceSpans"].([]any); ok {
		for _, rsItem := range rsList {
			rsMap, ok := rsItem.(map[string]any)
			if !ok {
				continue
			}

			if old, found := rsMap["instrumentationLibrarySpans"]; found {
				rsMap["scopeSpans"] = old
				delete(rsMap, "instrumentationLibrarySpans")
			}

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

			if ssList, ok := rsMap["scopeSpans"].([]any); ok {
				for _, ssItem := range ssList {
					ssMap, ok := ssItem.(map[string]any)
					if !ok {
						continue
					}
					if _, hasScope := ssMap["scope"]; !hasScope {
						ssMap["scope"] = map[string]any{}
					}

					if spans, ok := ssMap["spans"].([]any); ok {
						for _, spanItem := range spans {
							spanMap, ok := spanItem.(map[string]any)
							if !ok {
								continue
							}

							if serviceName != "" {
								spanMap["serviceName"] = serviceName
							}

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

func Run(db *sql.DB) {
	dialect := goqu.Dialect("default")
	telService := TelemetryCollectorService{
		Ch: db,
		DB: &dialect,
	}
	telController := TelemetryCollectorController{
		service: telService,
	}

	r := chi.NewRouter()

	telController.RegisterRoutes(r)
	addr := ":4318"
	log.Printf("listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}
