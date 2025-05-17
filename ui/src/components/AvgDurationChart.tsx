import React from 'react';
import {
  Card,
  CardContent,
  Typography,
  Box
} from '@mui/material';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend
} from 'recharts';

interface AvgDurationChartProps {
  data: { timestamp: string; value: number }[];
}

const AvgDurationChart: React.FC<AvgDurationChartProps> = ({ data }) => (
  <Card>
    <CardContent>
      <Typography variant="h6" gutterBottom>Avg Duration Over Time</Typography>
      <Box height={300}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="timestamp" tickFormatter={(v) => new Date(v).toLocaleString()} />
            <YAxis domain={[0, 'auto']} />
            <Tooltip
              labelFormatter={(v) => new Date(v).toLocaleString()}
              formatter={(value: number) => value.toFixed(2)}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey="value"
              name="Avg Duration (ms)"
              stroke="#82ca9d"
            />
          </LineChart>
        </ResponsiveContainer>
      </Box>
    </CardContent>
  </Card>
);

export default AvgDurationChart;
