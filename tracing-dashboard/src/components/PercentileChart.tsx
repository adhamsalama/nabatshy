import React from 'react';
import { Card, CardContent, Typography, Box } from '@mui/material';
import {
  ResponsiveContainer,
  LineChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip as ReTooltip,
  Legend,
  Line,
} from 'recharts';

export interface TimePercentile {
  timestamp: string; // ISO string or Date-compatible
  value: number;
}

interface PercentileChartProps {
  data: TimePercentile[];
  percentile: number; // e.g. 95
}

const PercentileChart: React.FC<PercentileChartProps> = ({ data, percentile }) => {
  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>
          P{percentile} Duration Over Time
        </Typography>
        <Box height={300}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="timestamp"
                tickFormatter={v => new Date(v).toLocaleString()}
              />
              <YAxis domain={[0, 'auto']} />
              <ReTooltip
                labelFormatter={v => new Date(v).toLocaleString()}
                formatter={val => [`${(val as number).toFixed(2)} ms`, `P${percentile}`]}
              />
              <Legend />
              <Line
                type="monotone"
                dataKey="value"
                name={`P${percentile} Duration (ms)`}
                stroke="#ff7300"
              />
            </LineChart>
          </ResponsiveContainer>
        </Box>
      </CardContent>
    </Card>
  );
};

export default PercentileChart;
