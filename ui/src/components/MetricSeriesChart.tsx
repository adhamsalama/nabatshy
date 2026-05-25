import React from 'react';
import { Card, CardContent, Typography, Box } from '@mui/material';
import { useTheme } from '@mui/material/styles';
import {
  ResponsiveContainer, LineChart, CartesianGrid, XAxis, YAxis,
  Tooltip as ReTooltip, Legend, Line, ReferenceArea,
} from 'recharts';
import { useChartBrush } from '../hooks/useChartBrush';

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

function yLabel(metricType: string, unit: string): string {
  if (metricType === 'histogram') return unit ? `avg (${unit})` : 'avg';
  return unit || '';
}

const MetricSeriesChart: React.FC<Props> = ({ series, unit, metricType, title, onRangeSelect }) => {
  const theme = useTheme();
  const tooltipStyle = {
    backgroundColor: theme.palette.background.paper,
    border: `1px solid ${theme.palette.divider}`,
    color: theme.palette.text.primary,
  };
  const { onMouseDown, onMouseMove, onMouseUp, refLeft, refRight, selecting } = useChartBrush(onRangeSelect);

  const data = buildChartData(series);
  const keys = series.map(s => seriesKey(s.labels));
  const unitLabel = yLabel(metricType, unit);

  return (
    <Card>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="h6">{title || 'Metric Over Time'}</Typography>
          {onRangeSelect && (
            <Typography variant="caption" color="text.secondary">drag to zoom</Typography>
          )}
        </Box>
        <Box
          height={320}
          sx={onRangeSelect ? { cursor: selecting ? 'col-resize' : 'crosshair', userSelect: 'none' } : {}}
        >
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={data}
              onMouseDown={onMouseDown}
              onMouseMove={onMouseMove}
              onMouseUp={onMouseUp}
              onMouseLeave={onMouseUp}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="time"
                tickFormatter={v => new Date(v as string).toLocaleTimeString()}
                minTickGap={40}
              />
              <YAxis
                domain={[0, 'auto']}
                width={70}
                tickFormatter={v => {
                  if (typeof v !== 'number') return String(v);
                  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
                  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(1)}k`;
                  if (Math.abs(v) < 0.001 && v !== 0) return v.toExponential(1);
                  return v.toFixed(v < 1 ? 3 : 1);
                }}
                label={unitLabel ? { value: unitLabel, angle: -90, position: 'insideLeft', offset: 10, style: { fontSize: 11, fill: theme.palette.text.secondary } } : undefined}
              />
              {!selecting && (
                <ReTooltip
                  contentStyle={tooltipStyle}
                  labelFormatter={v => new Date(v as string).toLocaleString()}
                  formatter={(val, name) => [
                    typeof val === 'number' ? `${val.toFixed(4)}${unitLabel ? ' ' + unitLabel : ''}` : '—',
                    name,
                  ]}
                />
              )}
              <Legend />
              {keys.map((key, i) => (
                <Line
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={COLORS[i % COLORS.length]}
                  dot={false}
                  connectNulls
                  strokeWidth={1.5}
                />
              ))}
              {selecting && refLeft && refRight && (
                <ReferenceArea x1={refLeft} x2={refRight} fill={COLORS[0]} fillOpacity={0.2} strokeOpacity={0.5} />
              )}
            </LineChart>
          </ResponsiveContainer>
        </Box>
      </CardContent>
    </Card>
  );
};

export default MetricSeriesChart;
