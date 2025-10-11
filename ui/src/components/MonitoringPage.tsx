import React, { useState, useEffect } from 'react';
import {
  Box,
  TextField,
  Typography,
  CircularProgress,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Button,
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import { format } from 'date-fns';
import PercentileChart, { TimePercentile } from './PercentileChart';
import TraceCountChart from './TraceCountChart';
import AvgDurationChart from './AvgDurationChart';
import ErrorCountChart from './ErrorCountChart';
import { config } from "../config.ts";

const percentileOptions = [50, 75, 90, 95, 99, 100] as const;

export const MonitoringPage: React.FC = () => {
  const [percentileSeries, setPercentileSeries] = useState<TimePercentile[]>([]);
  const [traceCountSeries, setTraceCountSeries] = useState<TimePercentile[]>([]);
  const [avgDurationSeries, setAvgDurationSeries] = useState<TimePercentile[]>([]);
  const [errorCountSeries, setErrorCountSeries] = useState<TimePercentile[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [percentile, setPercentile] = useState<number>(95);
  const [startDate, setStartDate] = useState(() => new Date(Date.now() - 60 * 60 * 1000)); // Last 1 hour
  const [endDate, setEndDate] = useState(() => new Date());
  const [selectedService, setSelectedService] = useState<string>('');
  const [availableServices, setAvailableServices] = useState<string[]>([]);
  const [traceOrSpan, setTraceOrSpan] = useState<'trace' | 'span'>('trace');

  // Fetch available services on mount
  useEffect(() => {
    const fetchServices = async () => {
      try {
        const response = await fetch(`${config.backendUrl}/api/services`);
        if (response.ok) {
          const services = await response.json();
          setAvailableServices(services || []);
        }
      } catch (err) {
        console.error('Failed to fetch services:', err);
      }
    };
    fetchServices();
  }, []);

  const fetchMetrics = async () => {
    if (!startDate || !endDate || isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      setError('Invalid start or end date');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const params = {
        start: startDate.toISOString(),
        end: endDate.toISOString(),
      };

      // Use /api/metrics/search endpoint for all cases (with or without service filter)
      const metricsUrl = new URL(`${config.backendUrl}/api/metrics/search`);
      if (selectedService) {
        metricsUrl.searchParams.set('query', `service.name=${selectedService}`);
      }
      metricsUrl.searchParams.set('start', params.start);
      metricsUrl.searchParams.set('end', params.end);
      metricsUrl.searchParams.set('percentile', String(percentile));
      metricsUrl.searchParams.set('traceOrSpan', traceOrSpan);

      const metricsResponse = await fetch(metricsUrl.toString());
      if (metricsResponse.ok) {
        const metricsData = await metricsResponse.json();
        setPercentileSeries(metricsData.PercentileResults || []);
        setTraceCountSeries(metricsData.TraceCountResults || []);
        setAvgDurationSeries(metricsData.AvgDurationResults || []);
      }

      // Error count still needs separate fetch
      const errorCountUrl = new URL(`${config.backendUrl}/api/metrics/errors`);
      errorCountUrl.searchParams.set('start', params.start);
      errorCountUrl.searchParams.set('end', params.end);

      const errorCountResponse = await fetch(errorCountUrl.toString());
      if (errorCountResponse.ok) {
        const errorCountData = await errorCountResponse.json();
        setErrorCountSeries(errorCountData || []);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setPercentileSeries([]);
      setTraceCountSeries([]);
      setAvgDurationSeries([]);
      setErrorCountSeries([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMetrics();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    fetchMetrics();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [percentile]);

  useEffect(() => {
    fetchMetrics();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedService]);

  useEffect(() => {
    fetchMetrics();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [traceOrSpan]);

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" gutterBottom>
        System Monitoring
      </Typography>

      <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center', mb: 3 }}>
        <TextField
          label="Start Time"
          type="datetime-local"
          value={format(startDate, "yyyy-MM-dd'T'HH:mm")}
          onChange={e => setStartDate(new Date(e.target.value))}
          InputLabelProps={{ shrink: true }}
        />
        <TextField
          label="End Time"
          type="datetime-local"
          value={format(endDate, "yyyy-MM-dd'T'HH:mm")}
          onChange={e => setEndDate(new Date(e.target.value))}
          InputLabelProps={{ shrink: true }}
        />
        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel>Percentile</InputLabel>
          <Select
            value={percentile}
            label="Percentile"
            onChange={e => setPercentile(Number(e.target.value))}
          >
            {percentileOptions.map(p => (
              <MenuItem key={`p${p}`} value={p}>{`P${p}`}</MenuItem>
            ))}
          </Select>
        </FormControl>
        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel>Service</InputLabel>
          <Select
            value={selectedService}
            label="Service"
            onChange={e => setSelectedService(e.target.value)}
          >
            <MenuItem value="">All Services</MenuItem>
            {availableServices.map(service => (
              <MenuItem key={service} value={service}>{service}</MenuItem>
            ))}
          </Select>
        </FormControl>
        <FormControl size="small" sx={{ minWidth: 150 }}>
          <InputLabel>Type</InputLabel>
          <Select
            value={traceOrSpan}
            label="Type"
            onChange={e => setTraceOrSpan(e.target.value as 'trace' | 'span')}
          >
            <MenuItem value="trace">Trace</MenuItem>
            <MenuItem value="span">Span</MenuItem>
          </Select>
        </FormControl>
        <Button
          variant="outlined"
          startIcon={<RefreshIcon />}
          onClick={fetchMetrics}
          disabled={loading}
        >
          Refresh
        </Button>
      </Box>

      {error && (
        <Box sx={{ mb: 2 }}>
          <Typography color="error">{error}</Typography>
        </Box>
      )}

      {loading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', my: 4 }}>
          <CircularProgress />
        </Box>
      )}

      {!loading && (
        <Box
          sx={{
            display: 'grid',
            gridTemplateColumns: 'repeat(2, 1fr)',
            gap: 2,
          }}
        >
          <Box>
            <PercentileChart data={percentileSeries} percentile={percentile} />
          </Box>
          <Box>
            <TraceCountChart data={traceCountSeries} />
          </Box>
          <Box>
            <AvgDurationChart data={avgDurationSeries} />
          </Box>
          <Box>
            <ErrorCountChart data={errorCountSeries} />
          </Box>
        </Box>
      )}
    </Box>
  );
};
