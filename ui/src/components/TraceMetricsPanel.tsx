import React, { useState, useCallback } from 'react';
import { Box, Button, Typography, CircularProgress } from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import MetricSeriesChart, { MetricSeries } from './MetricSeriesChart';
import { config } from '../config';

interface Props {
  startTimeNs: number;
  endTimeNs: number;
  serviceName?: string;
}

const WINDOW_MS = 5 * 60 * 1000;

const GROUP_BY: Record<string, string> = {
  'system.cpu.time': 'state',
  'system.cpu.utilization': 'state',
  'system.network.io': 'direction',
  'system.network.errors': 'direction',
  'system.network.dropped': 'direction',
};

interface MetricChart {
  name: string;
  unit: string;
  metricType: string;
  series: MetricSeries[];
}

const TraceMetricsPanel: React.FC<Props> = ({ startTimeNs, endTimeNs }) => {
  const [expanded, setExpanded] = useState(false);
  const [loading, setLoading] = useState(false);
  const [charts, setCharts] = useState<MetricChart[] | null>(null);

  const referenceTime = new Date(startTimeNs / 1e6).toISOString();
  const windowStart = new Date(startTimeNs / 1e6 - WINDOW_MS).toLocaleString();
  const windowEnd = new Date(endTimeNs / 1e6 + WINDOW_MS).toLocaleString();

  const fetchMetrics = useCallback(async () => {
    setLoading(true);
    const start = new Date(startTimeNs / 1e6 - WINDOW_MS).toISOString();
    const end = new Date(endTimeNs / 1e6 + WINDOW_MS).toISOString();

    try {
      const namesRes = await fetch(`${config.backendUrl}/api/otel-metrics/names`);
      const nameRows: { metric_name: string; metric_type: string; metric_unit: string }[] = await namesRes.json();

      const results = await Promise.all(
        nameRows.map(async (row): Promise<MetricChart> => {
          const groupBy = GROUP_BY[row.metric_name] ?? '';
          const params = new URLSearchParams({ metric_name: row.metric_name, start, end });
          if (groupBy) params.set('group_by', groupBy);
          const res = await fetch(`${config.backendUrl}/api/otel-metrics/series?${params}`);
          const data = await res.json();
          return {
            name: row.metric_name,
            unit: data.unit ?? row.metric_unit ?? '',
            metricType: data.metric_type ?? row.metric_type ?? 'gauge',
            series: (data.series ?? []) as MetricSeries[],
          };
        })
      );

      setCharts(results.filter(c => c.series.length > 0));
    } finally {
      setLoading(false);
    }
  }, [startTimeNs, endTimeNs]);

  const handleExpand = () => {
    setExpanded(true);
    if (!charts) fetchMetrics();
  };

  return (
    <Box mt={4}>
      {!expanded ? (
        <Button variant="outlined" size="small" endIcon={<ExpandMoreIcon />} onClick={handleExpand}>
          Show System Metrics
        </Button>
      ) : (
        <Box>
          <Box display="flex" alignItems="center" gap={2} mb={2}>
            <Typography variant="h6">System Metrics</Typography>
            <Typography variant="caption" color="text.secondary">
              ±5 min around trace start · {windowStart} – {windowEnd}
            </Typography>
            <Button size="small" endIcon={<ExpandLessIcon />} onClick={() => setExpanded(false)}>
              Hide
            </Button>
          </Box>

          {loading ? (
            <Box display="flex" justifyContent="center" py={4}>
              <CircularProgress />
            </Box>
          ) : charts && charts.length > 0 ? (
            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 2 }}>
              {charts.map(chart => (
                <MetricSeriesChart
                  key={chart.name}
                  series={chart.series}
                  unit={chart.unit}
                  metricType={chart.metricType}
                  title={chart.name}
                  referenceTime={referenceTime}
                />
              ))}
            </Box>
          ) : (
            <Typography color="text.secondary">No metrics found for this time window.</Typography>
          )}
        </Box>
      )}
    </Box>
  );
};

export default TraceMetricsPanel;
