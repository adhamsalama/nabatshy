package collector

import (
	"nabatshy/utils"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

type LogsService struct {
	writer *LogWriter
}

func (s *LogsService) ingestLogs(req *collogs.ExportLogsServiceRequest) {
	for _, rl := range req.ResourceLogs {
		resAttrs := extractAttributes(rl.Resource.GetAttributes())
		resKeys, resValues := attrMapToSlices(resAttrs)
		serviceName := resAttrs["service.name"]

		for _, sl := range rl.ScopeLogs {
			scopeName := sl.Scope.GetName()
			var records []utils.LogRecord

			for _, lr := range sl.LogRecords {
				attrKeys, attrValues := attrMapToSlices(extractAttributes(lr.Attributes))
				records = append(records, utils.LogRecord{
					TimestampUnixNano:       int64(lr.TimeUnixNano),
					ObservedTimeUnixNano:    int64(lr.ObservedTimeUnixNano),
					SeverityText:            lr.SeverityText,
					SeverityNumber:          int32(lr.SeverityNumber),
					Body:                    lr.Body.GetStringValue(),
					TraceID:                 encodeBytes(lr.TraceId),
					SpanID:                  encodeBytes(lr.SpanId),
					ServiceName:             serviceName,
					AttributesKey:           attrKeys,
					AttributesValue:         attrValues,
					ResourceAttributesKey:   resKeys,
					ResourceAttributesValue: resValues,
					ScopeName:               scopeName,
				})
			}

			if len(records) > 0 {
				s.writer.Write(records)
			}
		}
	}
}
