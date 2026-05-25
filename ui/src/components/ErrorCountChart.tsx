import React from 'react';
import { Card, CardContent, Typography, Box } from '@mui/material';
import { useTheme } from '@mui/material/styles';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceArea,
} from 'recharts';
import { useChartBrush } from '../hooks/useChartBrush';

interface ErrorCountChartProps {
  data: { timestamp: string; value: number }[];
  onRangeSelect?: (start: string, end: string) => void;
}

const ErrorCountChart: React.FC<ErrorCountChartProps> = ({ data, onRangeSelect }) => {
  const theme = useTheme();
  const tooltipStyle = { backgroundColor: theme.palette.background.paper, border: `1px solid ${theme.palette.divider}`, color: theme.palette.text.primary };
  const { onMouseDown, onMouseMove, onMouseUp, refLeft, refRight, selecting } = useChartBrush(onRangeSelect);

  return (
    <Card>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
          <Typography variant="h6">Error Count Over Time</Typography>
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
              <YAxis domain={[0, 'auto']} />
              {!selecting && <Tooltip contentStyle={tooltipStyle} labelFormatter={(v) => new Date(v).toLocaleString()} />}
              <Legend />
              <Line type="monotone" dataKey="value" name="Error Count" stroke="#f44336" dot={false} />
              {selecting && refLeft && refRight && (
                <ReferenceArea x1={refLeft} x2={refRight} fill="#f44336" fillOpacity={0.25} strokeOpacity={0.5} />
              )}
            </LineChart>
          </ResponsiveContainer>
        </Box>
      </CardContent>
    </Card>
  );
};

export default ErrorCountChart;
