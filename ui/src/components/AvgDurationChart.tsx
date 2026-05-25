import React from 'react';
import { Card, CardContent, Typography, Box } from '@mui/material';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceArea,
} from 'recharts';
import { useChartBrush } from '../hooks/useChartBrush';

interface AvgDurationChartProps {
  data: { timestamp: string; value: number }[];
  onRangeSelect?: (start: string, end: string) => void;
}

const AvgDurationChart: React.FC<AvgDurationChartProps> = ({ data, onRangeSelect }) => {
  const { onMouseDown, onMouseMove, onMouseUp, refLeft, refRight, selecting } = useChartBrush(onRangeSelect);

  return (
    <Card>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="h6">Avg Duration Over Time</Typography>
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
              <XAxis dataKey="timestamp" tickFormatter={(v) => new Date(v).toLocaleString()} />
              <YAxis domain={[0, 'auto']} unit="ms" width={60} />
              {!selecting && (
                <Tooltip
                  labelFormatter={(v) => new Date(v).toLocaleString()}
                  formatter={(value: number) => value.toFixed(2)}
                />
              )}
              <Legend />
              <Line type="monotone" dataKey="value" name="Avg Duration (ms)" stroke="#82ca9d" dot={false} />
              {selecting && refLeft && refRight && (
                <ReferenceArea x1={refLeft} x2={refRight} fill="#82ca9d" fillOpacity={0.25} strokeOpacity={0.5} />
              )}
            </LineChart>
          </ResponsiveContainer>
        </Box>
      </CardContent>
    </Card>
  );
};

export default AvgDurationChart;
