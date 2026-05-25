import React from 'react';
import { Card, CardContent, Typography, Box } from '@mui/material';
import { useTheme } from '@mui/material/styles';
import {
  ResponsiveContainer,
  LineChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip as ReTooltip,
  Legend,
  Line,
  ReferenceArea,
} from 'recharts';
import { useChartBrush } from '../hooks/useChartBrush';

export interface TimePercentile {
  timestamp: string;
  value: number;
}

interface PercentileChartProps {
  data: TimePercentile[];
  percentile: number;
  onRangeSelect?: (start: string, end: string) => void;
}

const PercentileChart: React.FC<PercentileChartProps> = ({ data, percentile, onRangeSelect }) => {
  const theme = useTheme();
  const tooltipStyle = { backgroundColor: theme.palette.background.paper, border: `1px solid ${theme.palette.divider}`, color: theme.palette.text.primary };
  const { onMouseDown, onMouseMove, onMouseUp, refLeft, refRight, selecting } = useChartBrush(onRangeSelect);

  return (
    <Card>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="h6">P{percentile} Duration Over Time</Typography>
          {onRangeSelect && (
            <Typography variant="caption" color="text.secondary">drag to zoom into a range</Typography>
          )}
        </Box>
        <Box height={300} sx={onRangeSelect ? { cursor: selecting ? 'col-resize' : 'crosshair', userSelect: 'none' } : {}}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={data}
              onMouseDown={onMouseDown}
              onMouseMove={onMouseMove}
              onMouseUp={onMouseUp}
              onMouseLeave={onMouseUp}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="timestamp" tickFormatter={v => new Date(v).toLocaleString()} />
              <YAxis domain={[0, 'auto']} unit="ms" width={60} />
              {!selecting && (
                <ReTooltip
                  contentStyle={tooltipStyle}
                  labelFormatter={v => new Date(v).toLocaleString()}
                  formatter={val => [`${(val as number).toFixed(2)} ms`, `P${percentile}`]}
                />
              )}
              <Legend />
              <Line type="monotone" dataKey="value" name={`P${percentile} Duration (ms)`} stroke="#ff7300" dot={false} />
              {selecting && refLeft && refRight && (
                <ReferenceArea x1={refLeft} x2={refRight} fill="#ff7300" fillOpacity={0.25} strokeOpacity={0.5} />
              )}
            </LineChart>
          </ResponsiveContainer>
        </Box>
      </CardContent>
    </Card>
  );
};

export default PercentileChart;
