import React from 'react';
import { Card, CardContent, Typography, Box } from '@mui/material';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

interface TraceCountChartProps {
  data: { timestamp: string; value: number }[];
}

const TraceCountChart: React.FC<TraceCountChartProps> = ({ data }) => {
  return (
    <Card>
      <CardContent>
        <Typography variant="h6" gutterBottom>Trace Count Over Time</Typography>
        <Box height={300}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="timestamp" tickFormatter={(v) => new Date(v).toLocaleString()} />
              <YAxis domain={[0, 'auto']} />
              <Tooltip labelFormatter={(v) => new Date(v).toLocaleString()} />
              <Legend />
              <Line type="monotone" dataKey="value" name="Trace Count" stroke="#8884d8" />
            </LineChart>
          </ResponsiveContainer>
        </Box>
      </CardContent>
    </Card>
  );
};

export default TraceCountChart;
