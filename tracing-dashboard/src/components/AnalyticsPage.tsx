import React, { useEffect, useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  CircularProgress,
  SelectChangeEvent,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
} from '@mui/material';
import { DateTimePicker } from '@mui/x-date-pickers/DateTimePicker';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { AdapterDateFns } from '@mui/x-date-pickers/AdapterDateFns';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  BarChart,
  Bar,
} from 'recharts';

interface TimeRangeMetrics {
  timestamp: string;
  count: number;
  avg_duration_ms: number;
  error_rate: number;
}

interface ServiceMetrics {
  service: string;
  count: number;
  avg_duration_ms: number;
  error_rate: number;
}

interface EndpointMetrics {
  endpoint: string;
  count: number;
  avg_duration_ms: number;
  p95_duration_ms: number;
}
interface GraphData {
  timestamp: string;
  value: number;
}
// Series point for p-percentile
interface PercentilePoint extends GraphData {
}

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8'];

const AnalyticsPage: React.FC = () => {
  const [timeRange, setTimeRange] = useState('24h');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [traceMetrics, setTraceMetrics] = useState<TimeRangeMetrics[]>([]);
  const [serviceMetrics, setServiceMetrics] = useState<ServiceMetrics[]>([]);
  const [avgMetrics, setAvgMetrics] = useState<GraphData[]>([]);
  const [percentileSeries, setPercentileSeries] = useState<PercentilePoint[]>([]);

  // Customize percentile and bucket count
  const P = 95;
  const BUCKETS = 10;

  const [customDateDialogOpen, setCustomDateDialogOpen] = useState(false);
  const [startDate, setStartDate] = useState<Date | null>(null);
  const [endDate, setEndDate] = useState<Date | null>(null);
  const [tempStartDate, setTempStartDate] = useState<Date | null>(null);
  const [tempEndDate, setTempEndDate] = useState<Date | null>(null);

  const fetchMetrics = async () => {
    setLoading(true);
    setError(null);
    try {
      const baseUrl = 'http://localhost:4318/api/metrics';
      let params = '';

      if (timeRange === 'custom' && startDate && endDate) {
        params = `?start=${startDate.toISOString()}&end=${endDate.toISOString()}`;
      } else {
        params = `?timeRange=${timeRange}`;
      }

      const [
        traceResponse,
        serviceResponse,
        avgResponse
      ] = await Promise.all([
        fetch(`${baseUrl}/traces${params}`).then(res => res.json()),
        fetch(`${baseUrl}/services${params}`).then(res => res.json()),
        fetch(`${baseUrl}/avg${params}`).then(res => res.json())
      ]);

      setTraceMetrics(Array.isArray(traceResponse) ? traceResponse : []);
      setServiceMetrics(Array.isArray(serviceResponse) ? serviceResponse : []);
      setAvgMetrics(Array.isArray(avgResponse) ? avgResponse : []);

      // Fetch percentile series
      const sep = params.includes('?') ? '&' : '?';
      const seriesResponse = await fetch(
        `${baseUrl}/pseries${params}${sep}percentile=${P}&buckets=${BUCKETS}`
      ).then(res => res.json());
      setPercentileSeries(Array.isArray(seriesResponse) ? seriesResponse : []);

    } catch (err) {
      console.error('Error fetching metrics:', err);
      setError(err instanceof Error ? err.message : 'An error occurred');
      setTraceMetrics([]);
      setServiceMetrics([]);
      setAvgMetrics([]);
      setPercentileSeries([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (timeRange === 'custom' && !startDate && !endDate) return;
    fetchMetrics();
  }, [timeRange, startDate, endDate]);

  const handleTimeRangeChange = (event: SelectChangeEvent) => {
    const value = event.target.value;
    if (value === 'custom') {
      setCustomDateDialogOpen(true);
      setTempStartDate(startDate);
      setTempEndDate(endDate);
    } else {
      setTimeRange(value);
      setStartDate(null);
      setEndDate(null);
    }
  };

  const handleCustomDateSubmit = () => {
    if (tempStartDate && tempEndDate) {
      setStartDate(tempStartDate);
      setEndDate(tempEndDate);
      setTimeRange('custom');
      setCustomDateDialogOpen(false);
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="80vh">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box p={3}>
        <Typography color="error">{error}</Typography>
      </Box>
    );
  }

  return (
    <Box p={3}>
      <Box mb={3} display="flex" justifyContent="space-between" alignItems="center">
        <Typography variant="h4">Analytics</Typography>
        <FormControl sx={{ minWidth: 200 }}>
          <InputLabel>Time Range</InputLabel>
          <Select value={timeRange} label="Time Range" onChange={handleTimeRangeChange}>
            <MenuItem value="1h">Last Hour</MenuItem>
            <MenuItem value="24h">Last 24 Hours</MenuItem>
            <MenuItem value="7d">Last 7 Days</MenuItem>
            <MenuItem value="30d">Last 30 Days</MenuItem>
            <MenuItem value="custom">Custom Range</MenuItem>
          </Select>
        </FormControl>
      </Box>

      <Dialog open={customDateDialogOpen} onClose={() => setCustomDateDialogOpen(false)}>
        <DialogTitle>Select Custom Time Range</DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 2 }}>
            <LocalizationProvider dateAdapter={AdapterDateFns}>
              <DateTimePicker
                label="Start Date & Time"
                value={tempStartDate}
                onChange={(newVal) => setTempStartDate(newVal)}
                maxDateTime={tempEndDate || undefined}
                slotProps={{ textField: { fullWidth: true } }}
              />
              <DateTimePicker
                label="End Date & Time"
                value={tempEndDate}
                onChange={(newVal) => setTempEndDate(newVal)}
                minDateTime={tempStartDate || undefined}
                slotProps={{ textField: { fullWidth: true } }}
              />
            </LocalizationProvider>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCustomDateDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleCustomDateSubmit} disabled={!tempStartDate || !tempEndDate}>
            Apply
          </Button>
        </DialogActions>
      </Dialog>

      <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(12, 1fr)', gap: 3 }}>
        {/* Trace Count Chart */}
        <Box sx={{ gridColumn: 'span 12' }}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>Trace Count Over Time</Typography>
              <Box height={300}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={traceMetrics}>
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
        </Box>

        {/* Avg Duration Chart */}
        <Box sx={{ gridColumn: 'span 12' }}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>Avg Duration Over Time</Typography>
              <Box height={300}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={avgMetrics}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="timestamp" tickFormatter={(v) => new Date(v).toLocaleString()} />
                    <YAxis domain={[0, 'auto']} />
                    <Tooltip labelFormatter={(v) => new Date(v).toLocaleString()}
                      formatter={(value: number) => value.toFixed(2)}
                    />
                    <Legend />
                    <Line type="monotone" dataKey="value" name="Avg Duration (ms)" stroke="#82ca9d" />
                  </LineChart>
                </ResponsiveContainer>
              </Box>
            </CardContent>
          </Card>
        </Box>

        {/* P95 Series Chart */}
        <Box sx={{ gridColumn: 'span 12' }}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>P{P} Duration Over Time</Typography>
              <Box height={300}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={percentileSeries}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="timestamp" tickFormatter={(v) => new Date(v).toLocaleString()} />
                    <YAxis domain={[0, 'auto']} />
                    <Tooltip labelFormatter={(v) => new Date(v).toLocaleString()} formatter={(val) => [`${val.toFixed(2)} ms`, `P${P}`]} />
                    <Legend />
                    <Line type="monotone" dataKey="value" name={`P${P} Duration (ms)`} stroke="#ff7300" />
                  </LineChart>
                </ResponsiveContainer>
              </Box>
            </CardContent>
          </Card>
        </Box>

        {/* Service Distribution Chart */}
        <Box sx={{ gridColumn: { xs: 'span 12', md: 'span 6' } }}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>Service Distribution</Typography>
              <Box height={400}>
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie data={serviceMetrics} dataKey="count" nameKey="service" cx="50%" cy="50%" outerRadius={150} label>
                      {serviceMetrics.map((entry, index) => (<Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />))}
                    </Pie>
                    <Tooltip />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              </Box>
            </CardContent>
          </Card>
        </Box>

        {/* ... other charts unchanged ... */}
      </Box>
    </Box>
  );
};

export default AnalyticsPage;

