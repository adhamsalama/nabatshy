import React from 'react';
import { Card, CardContent, Typography, Box } from '@mui/material';
import { useTheme } from '@mui/material/styles';
import {
  ResponsiveContainer,
  LineChart, Line,
  AreaChart, Area,
  CartesianGrid, XAxis, YAxis,
  Tooltip as ReTooltip, Legend,
  ReferenceArea, ReferenceLine,
} from 'recharts';
import { useChartBrush } from '../hooks/useChartBrush';
import { resolveUnit, formatMetricValue } from '../utils/metricUnits';

const COLORS = ['#2C6B6B', '#ff7300', '#8884d8', '#82ca9d', '#ffc658', '#a4de6c', '#fa8072', '#b0c4de'];

export interface SeriesPoint {
  time: number;
  value: number;
  histogram_count?: number;
  histogram_sum?: number;
}

export interface MetricSeries {
  labels: Record<string, string>;
  points: SeriesPoint[];
}

interface Props {
  series: MetricSeries[];
  unit: string;
  metricType: string;
  title?: string;
  onRangeSelect?: (start: string, end: string) => void;
  referenceTime?: string;
  metricName?: string;
}

function seriesKey(labels: Record<string, string>): string {
  const entries = Object.entries(labels);
  if (entries.length === 0) return 'value';
  return entries.map(([, v]) => v).join(' · ');
}

function buildChartData(series: MetricSeries[]): Record<string, unknown>[] {
  const timeSet = new Set<number>();
  series.forEach(s => s.points.forEach(p => timeSet.add(p.time)));
  const times = Array.from(timeSet).sort((a, b) => a - b);

  return times.map(t => {
    const row: Record<string, unknown> = { time: new Date(t).toISOString() };
    series.forEach(s => {
      const key = seriesKey(s.labels);
      const pt = s.points.find(p => p.time === t);
      row[key] = pt !== undefined ? pt.value : null;
    });
    return row;
  });
}

const MetricSeriesChart: React.FC<Props> = ({ series, unit, metricType, title, onRangeSelect, referenceTime, metricName }) => {
  const resolvedUnit = resolveUnit(metricName ?? '', unit);
  const tickFmt = (v: unknown) => {
    if (typeof v !== 'number') return String(v);
    return formatMetricValue(v, resolvedUnit);
  };
  const theme = useTheme();
  const tooltipStyle = {
    backgroundColor: theme.palette.background.paper,
    border: `1px solid ${theme.palette.divider}`,
    color: theme.palette.text.primary,
  };
  const { onMouseDown, onMouseMove, onMouseUp, refLeft, refRight, selecting } = useChartBrush(onRangeSelect);

  const data = buildChartData(series);
  const keys = series.map(s => seriesKey(s.labels));
  const brushProps = { onMouseDown, onMouseMove, onMouseUp, onMouseLeave: onMouseUp };

  const yAxis = (
    <YAxis
      domain={[0, 'auto']}
      width={70}
      tickFormatter={tickFmt}
      label={resolvedUnit ? { value: resolvedUnit, angle: -90, position: 'insideLeft', offset: 10, style: { fontSize: 11, fill: theme.palette.text.secondary } } : undefined}
    />
  );

  const xAxis = (
    <XAxis
      dataKey="time"
      tickFormatter={v => new Date(v as string).toLocaleTimeString()}
      minTickGap={40}
    />
  );

  const tooltip = !selecting ? (
    <ReTooltip
      contentStyle={tooltipStyle}
      labelFormatter={v => new Date(v as string).toLocaleString()}
      formatter={(val, name: string) => [
        typeof val === 'number' ? formatMetricValue(val, resolvedUnit) : '—',
        name,
      ]}
    />
  ) : null;

  const refArea = selecting && refLeft && refRight ? (
    <ReferenceArea x1={refLeft} x2={refRight} fill={COLORS[0]} fillOpacity={0.2} strokeOpacity={0.5} />
  ) : null;

  const refLine = referenceTime ? (
    <ReferenceLine
      x={referenceTime}
      stroke="#ef4444"
      strokeDasharray="4 2"
      label={{ value: 'trace', position: 'top', fontSize: 10, fill: '#ef4444' }}
    />
  ) : null;

  const cursorStyle = onRangeSelect
    ? { cursor: selecting ? 'col-resize' : 'crosshair', userSelect: 'none' as const }
    : {};

  return (
    <Card>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="h6">{title || 'Metric Over Time'}</Typography>
          <Typography variant="caption" color="text.secondary">
            {metricType === 'gauge' && 'line · instantaneous value'}
            {metricType === 'sum' && 'area · cumulative value'}
            {metricType === 'histogram' && 'line · avg value (sum/count)'}
            {onRangeSelect && ' · drag to zoom'}
          </Typography>
        </Box>

        <Box height={320} sx={cursorStyle}>
          <ResponsiveContainer width="100%" height="100%">

            {/* ── gauge: line chart ── */}
            {metricType === 'gauge' ? (
              <LineChart data={data} {...brushProps}>
                <CartesianGrid strokeDasharray="3 3" />
                {xAxis}{yAxis}{tooltip}<Legend />
                {keys.map((key, i) => (
                  <Line key={key} type="monotone" dataKey={key} stroke={COLORS[i % COLORS.length]} dot={false} connectNulls strokeWidth={1.5} />
                ))}
                {refArea}{refLine}
              </LineChart>

            /* ── sum: area chart ── */
            ) : metricType === 'sum' ? (
              <AreaChart data={data} {...brushProps}>
                <CartesianGrid strokeDasharray="3 3" />
                {xAxis}{yAxis}{tooltip}<Legend />
                {keys.map((key, i) => (
                  <Area
                    key={key}
                    type="monotone"
                    dataKey={key}
                    stroke={COLORS[i % COLORS.length]}
                    fill={COLORS[i % COLORS.length]}
                    fillOpacity={0.15}
                    dot={false}
                    connectNulls
                    strokeWidth={1.5}
                  />
                ))}
                {refArea}{refLine}
              </AreaChart>

            /* ── histogram: line chart of avg value (sum/count) ── */
            ) : (
              <LineChart data={data} {...brushProps}>
                <CartesianGrid strokeDasharray="3 3" />
                {xAxis}{yAxis}{tooltip}<Legend />
                {keys.map((key, i) => (
                  <Line key={key} type="monotone" dataKey={key} stroke={COLORS[i % COLORS.length]} dot={false} connectNulls strokeWidth={1.5} />
                ))}
                {refArea}{refLine}
              </LineChart>
            )}

          </ResponsiveContainer>
        </Box>
      </CardContent>
    </Card>
  );
};

export default MetricSeriesChart;
