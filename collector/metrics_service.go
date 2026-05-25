package collector

import (
	"nabatshy/utils"

	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

type MetricsService struct {
	writer *MetricWriter
}

func (s *MetricsService) ingestMetrics(req *colmetrics.ExportMetricsServiceRequest) {
	for _, rm := range req.ResourceMetrics {
		resourceAttrs := extractAttributes(rm.Resource.GetAttributes())
		resKeys, resValues := attrMapToSlices(resourceAttrs)

		for _, sm := range rm.ScopeMetrics {
			scopeName := sm.Scope.GetName()

			var points []utils.MetricDataPoint
			for _, metric := range sm.Metrics {
				switch data := metric.Data.(type) {
				case *metricspb.Metric_Gauge:
					for _, dp := range data.Gauge.DataPoints {
						p := numberDataPoint(metric, "gauge", "", false, dp, resKeys, resValues, scopeName)
						points = append(points, p)
					}
				case *metricspb.Metric_Sum:
					temporality := data.Sum.AggregationTemporality.String()
					for _, dp := range data.Sum.DataPoints {
						p := numberDataPoint(metric, "sum", temporality, data.Sum.IsMonotonic, dp, resKeys, resValues, scopeName)
						points = append(points, p)
					}
				case *metricspb.Metric_Histogram:
					temporality := data.Histogram.AggregationTemporality.String()
					for _, dp := range data.Histogram.DataPoints {
						p := histogramDataPoint(metric, temporality, dp, resKeys, resValues, scopeName)
						points = append(points, p)
					}
				}
			}

			if len(points) > 0 {
				s.writer.Write(points)
			}
		}
	}
}

func numberDataPoint(
	metric *metricspb.Metric,
	metricType, temporality string,
	isMonotonic bool,
	dp *metricspb.NumberDataPoint,
	resKeys, resValues []string,
	scopeName string,
) utils.MetricDataPoint {
	attrKeys, attrValues := attrMapToSlices(extractAttributes(dp.Attributes))
	p := utils.MetricDataPoint{
		MetricName:              metric.Name,
		MetricDescription:       metric.Description,
		MetricUnit:              metric.Unit,
		MetricType:              metricType,
		TimeUnixNano:            int64(dp.TimeUnixNano),
		StartTimeUnixNano:       int64(dp.StartTimeUnixNano),
		AggregationTemporality:  temporality,
		IsMonotonic:             isMonotonic,
		AttributesKey:           attrKeys,
		AttributesValue:         attrValues,
		ResourceAttributesKey:   resKeys,
		ResourceAttributesValue: resValues,
		ScopeName:               scopeName,
	}
	switch v := dp.Value.(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		p.ValueDouble = v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		p.ValueInt = v.AsInt
	}
	return p
}

func histogramDataPoint(
	metric *metricspb.Metric,
	temporality string,
	dp *metricspb.HistogramDataPoint,
	resKeys, resValues []string,
	scopeName string,
) utils.MetricDataPoint {
	attrKeys, attrValues := attrMapToSlices(extractAttributes(dp.Attributes))

	bucketCounts := make([]int64, len(dp.BucketCounts))
	for i, c := range dp.BucketCounts {
		bucketCounts[i] = int64(c)
	}

	var histSum, histMin, histMax float64
	if dp.Sum != nil {
		histSum = *dp.Sum
	}
	if dp.Min != nil {
		histMin = *dp.Min
	}
	if dp.Max != nil {
		histMax = *dp.Max
	}

	return utils.MetricDataPoint{
		MetricName:              metric.Name,
		MetricDescription:       metric.Description,
		MetricUnit:              metric.Unit,
		MetricType:              "histogram",
		TimeUnixNano:            int64(dp.TimeUnixNano),
		StartTimeUnixNano:       int64(dp.StartTimeUnixNano),
		AggregationTemporality:  temporality,
		HistogramCount:          int64(dp.Count),
		HistogramSum:            histSum,
		HistogramMin:            histMin,
		HistogramMax:            histMax,
		HistogramBucketCounts:   bucketCounts,
		HistogramExplicitBounds: dp.ExplicitBounds,
		AttributesKey:           attrKeys,
		AttributesValue:         attrValues,
		ResourceAttributesKey:   resKeys,
		ResourceAttributesValue: resValues,
		ScopeName:               scopeName,
	}
}

func attrMapToSlices(m map[string]string) ([]string, []string) {
	keys := make([]string, 0, len(m))
	values := make([]string, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values
}

