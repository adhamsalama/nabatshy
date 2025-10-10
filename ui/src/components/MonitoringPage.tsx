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

      // If service is selected, use /v1/search endpoint with service filter
      if (selectedService) {
        const searchUrl = new URL(`${config.backendUrl}/v1/search`);
        searchUrl.searchParams.set('query', `service.name=${selectedService}`);
        searchUrl.searchParams.set('start', params.start);
        searchUrl.searchParams.set('end', params.end);
        searchUrl.searchParams.set('percentile', String(percentile));
        searchUrl.searchParams.set('page', '1');
        searchUrl.searchParams.set('pageSize', '1'); // We only need the metrics, not results

        const searchResponse = await fetch(searchUrl.toString());
        if (searchResponse.ok) {
          const searchData = await searchResponse.json();
          setPercentileSeries(searchData.percentile || []);
          setTraceCountSeries(searchData.traceCount || []);
          setAvgDurationSeries(searchData.avgDuration || []);
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
      } else {
        // No service selected, use individual endpoints (unfiltered)
        // Fetch percentile series
        const percentileUrl = new URL(`${config.backendUrl}/api/metrics/pseries`);
        percentileUrl.searchParams.set('start', params.start);
        percentileUrl.searchParams.set('end', params.end);
        percentileUrl.searchParams.set('percentile', String(percentile));

        const percentileResponse = await fetch(percentileUrl.toString());
        if (percentileResponse.ok) {
          const percentileData = await percentileResponse.json();
          setPercentileSeries(percentileData || []);
        }

        // Fetch trace count
        const traceCountUrl = new URL(`${config.backendUrl}/api/metrics/traces`);
        traceCountUrl.searchParams.set('start', params.start);
        traceCountUrl.searchParams.set('end', params.end);

        const traceCountResponse = await fetch(traceCountUrl.toString());
        if (traceCountResponse.ok) {
          const traceCountData = await traceCountResponse.json();
          setTraceCountSeries(traceCountData || []);
        }

        // Fetch average duration
        const avgDurationUrl = new URL(`${config.backendUrl}/api/metrics/avg`);
        avgDurationUrl.searchParams.set('start', params.start);
        avgDurationUrl.searchParams.set('end', params.end);

        const avgDurationResponse = await fetch(avgDurationUrl.toString());
        if (avgDurationResponse.ok) {
          const avgDurationData = await avgDurationResponse.json();
          setAvgDurationSeries(avgDurationData || []);
        }

        // Fetch error count
        const errorCountUrl = new URL(`${config.backendUrl}/api/metrics/errors`);
        errorCountUrl.searchParams.set('start', params.start);
        errorCountUrl.searchParams.set('end', params.end);

        const errorCountResponse = await fetch(errorCountUrl.toString());
        if (errorCountResponse.ok) {
          const errorCountData = await errorCountResponse.json();
          setErrorCountSeries(errorCountData || []);
        }
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
