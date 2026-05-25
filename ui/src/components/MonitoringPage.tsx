import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Box,
  Typography,
  CircularProgress,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Button,
  Card,
  CardContent,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  Drawer,
  IconButton,
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import CloseIcon from '@mui/icons-material/Close';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import PercentileChart, { TimePercentile } from './PercentileChart';
import TraceCountChart from './TraceCountChart';
import { TraceDetails } from './TraceDetails';
import AvgDurationChart from './AvgDurationChart';
import ErrorCountChart from './ErrorCountChart';
import { config } from '../config.ts';

const percentileOptions = [50, 75, 90, 95, 99, 100] as const;

const TIME_PRESETS = [
  { label: 'Last 5m',  minutes: 5 },
  { label: 'Last 15m', minutes: 15 },
  { label: 'Last 30m', minutes: 30 },
  { label: 'Last 1h',  minutes: 60 },
  { label: 'Last 3h',  minutes: 180 },
  { label: 'Last 24h', minutes: 1440 },
];

const REFRESH_INTERVALS = [
  { label: '10s', seconds: 10 },
  { label: '30s', seconds: 30 },
  { label: '1m',  seconds: 60 },
  { label: '5m',  seconds: 300 },
];

interface SlowTrace {
  trace_id: string;
  name: string;
  duration_ms: number;
  service: string;
  start_time: number; // nanoseconds
}

interface StatCardProps {
  label: string;
  value: string;
  sub?: string;
  color?: string;
}

const StatCard: React.FC<StatCardProps> = ({ label, value, sub, color }) => (
  <Card sx={{ flex: 1, minWidth: 140 }}>
    <CardContent sx={{ pb: '12px !important' }}>
      <Typography variant="caption" color="text.secondary" display="block">{label}</Typography>
      <Typography variant="h4" sx={{ fontWeight: 700, color: color ?? 'text.primary', lineHeight: 1.2, mt: 0.5 }}>
        {value}
      </Typography>
      {sub && <Typography variant="caption" color="text.secondary">{sub}</Typography>}
    </CardContent>
  </Card>
);

export const MonitoringPage: React.FC = () => {
  const [percentileSeries, setPercentileSeries] = useState<TimePercentile[]>([]);
  const [traceCountSeries, setTraceCountSeries] = useState<TimePercentile[]>([]);
  const [avgDurationSeries, setAvgDurationSeries] = useState<TimePercentile[]>([]);
  const [errorCountSeries, setErrorCountSeries] = useState<TimePercentile[]>([]);
  const [slowTraces, setSlowTraces] = useState<SlowTrace[]>([]);
  const [selectedTraceId, setSelectedTraceId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [percentile, setPercentile] = useState<number>(95);
  const [timePresetIdx, setTimePresetIdx] = useState(3); // Last 1h
  const [selectedService, setSelectedService] = useState<string>('');
  const [availableServices, setAvailableServices] = useState<string[]>([]);
  const [traceOrSpan, setTraceOrSpan] = useState<'trace' | 'span'>('trace');
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(30);

  const getDateRange = useCallback(() => {
    const end = new Date();
    const start = new Date(end.getTime() - TIME_PRESETS[timePresetIdx].minutes * 60 * 1000);
    return { start, end };
  }, [timePresetIdx]);

  const fetchMetrics = useCallback(async () => {
    const { start, end } = getDateRange();
    if (isNaN(start.getTime()) || isNaN(end.getTime())) { setError('Invalid date range'); return; }

    setLoading(true);
    setError(null);
    try {
      const metricsUrl = new URL(`${config.backendUrl}/api/metrics/search`);
      if (selectedService) metricsUrl.searchParams.set('query', `service.name=${selectedService}`);
      metricsUrl.searchParams.set('start', start.toISOString());
      metricsUrl.searchParams.set('end', end.toISOString());
      metricsUrl.searchParams.set('percentile', String(percentile));
      metricsUrl.searchParams.set('traceOrSpan', traceOrSpan);

      const errorUrl = new URL(`${config.backendUrl}/api/metrics/errors`);
      errorUrl.searchParams.set('start', start.toISOString());
      errorUrl.searchParams.set('end', end.toISOString());
      if (selectedService) errorUrl.searchParams.set('service', selectedService);

      const slowUrl = new URL(`${config.backendUrl}/v1/traces/slowest`);
      slowUrl.searchParams.set('n', '20');
      if (selectedService) slowUrl.searchParams.set('service', selectedService);

      const [metricsRes, errorRes, slowRes] = await Promise.all([
        fetch(metricsUrl.toString()),
        fetch(errorUrl.toString()),
        fetch(slowUrl.toString()),
      ]);

      if (metricsRes.ok) {
        const d = await metricsRes.json();
        setPercentileSeries(d.PercentileResults || []);
        setTraceCountSeries(d.TraceCountResults || []);
        setAvgDurationSeries(d.AvgDurationResults || []);
      }
      if (errorRes.ok) setErrorCountSeries(await errorRes.json() || []);
      if (slowRes.ok) setSlowTraces(await slowRes.json() || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  }, [getDateRange, selectedService, percentile, traceOrSpan]);

  useEffect(() => { fetchMetrics(); }, [fetchMetrics]);

  const fetchRef = useRef(fetchMetrics);
  useEffect(() => { fetchRef.current = fetchMetrics; });
  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(() => fetchRef.current(), refreshInterval * 1000);
    return () => clearInterval(id);
  }, [autoRefresh, refreshInterval]);

  useEffect(() => {
    fetch(`${config.backendUrl}/api/services`)
      .then(r => r.ok ? r.json() : [])
      .then(s => setAvailableServices(s || []))
      .catch(() => {});
  }, []);

  // Derived summary stats
  const latestP = [...percentileSeries].reverse().find(p => p.value > 0)?.value ?? null;
  const totalCount = traceCountSeries.reduce((s, p) => s + p.value, 0);
  const totalErrors = errorCountSeries.reduce((s, p) => s + p.value, 0);
  const errorRate = totalCount > 0 ? ((totalErrors / totalCount) * 100).toFixed(1) : '0.0';

  const fmtMs = (ms: number) => ms < 1000 ? `${ms.toFixed(0)}ms` : `${(ms / 1000).toFixed(2)}s`;

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h5" gutterBottom fontWeight={600}>Monitoring</Typography>

      {/* Toolbar */}
      <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center', mb: 3 }}>
        <FormControl size="small" sx={{ minWidth: 140 }}>
          <InputLabel>Time Range</InputLabel>
          <Select value={timePresetIdx} label="Time Range" onChange={e => setTimePresetIdx(Number(e.target.value))}>
            {TIME_PRESETS.map((p, i) => <MenuItem key={i} value={i}>{p.label}</MenuItem>)}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel>Percentile</InputLabel>
          <Select value={percentile} label="Percentile" onChange={e => setPercentile(Number(e.target.value))}>
            {percentileOptions.map(p => <MenuItem key={p} value={p}>P{p}</MenuItem>)}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 180 }}>
          <InputLabel shrink>Service</InputLabel>
          <Select value={selectedService} label="Service" onChange={e => setSelectedService(e.target.value)} displayEmpty renderValue={(v: string) => v || 'All Services'} notched>
            <MenuItem value="">All Services</MenuItem>
            {availableServices.map(s => <MenuItem key={s} value={s}>{s}</MenuItem>)}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 130 }}>
          <InputLabel>Type</InputLabel>
          <Select value={traceOrSpan} label="Type" onChange={e => setTraceOrSpan(e.target.value as 'trace' | 'span')}>
            <MenuItem value="trace">Trace</MenuItem>
            <MenuItem value="span">Span</MenuItem>
          </Select>
        </FormControl>

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography variant="body2">Auto-refresh</Typography>
          <Switch checked={autoRefresh} onChange={e => setAutoRefresh(e.target.checked)} size="small" />
        </Box>

        {autoRefresh && (
          <FormControl size="small" sx={{ minWidth: 100 }}>
            <InputLabel>Interval</InputLabel>
            <Select value={refreshInterval} label="Interval" onChange={e => setRefreshInterval(Number(e.target.value))}>
              {REFRESH_INTERVALS.map(r => <MenuItem key={r.seconds} value={r.seconds}>{r.label}</MenuItem>)}
            </Select>
          </FormControl>
        )}

        <Button variant="outlined" startIcon={<RefreshIcon />} onClick={fetchMetrics} disabled={loading}>
          Refresh
        </Button>
      </Box>

      {error && <Typography color="error" sx={{ mb: 2 }}>{error}</Typography>}

      {loading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', my: 4 }}>
          <CircularProgress />
        </Box>
      )}

      {!loading && (
        <>
          {/* Stat cards */}
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', mb: 3 }}>
            <StatCard
              label={`P${percentile} Latency`}
              value={latestP !== null ? fmtMs(latestP) : '—'}
              sub="latest bucket"
            />
            <StatCard
              label="Total Spans / Traces"
              value={totalCount.toLocaleString()}
              sub={TIME_PRESETS[timePresetIdx].label}
            />
            <StatCard
              label="Total Errors"
              value={totalErrors.toLocaleString()}
              sub="exceptions"
              color={totalErrors > 0 ? '#ef4444' : undefined}
            />
            <StatCard
              label="Error Rate"
              value={`${errorRate}%`}
              color={parseFloat(errorRate) > 5 ? '#ef4444' : parseFloat(errorRate) > 1 ? '#f59e0b' : undefined}
            />
          </Box>

          {/* Charts */}
          <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 2, mb: 3 }}>
            <PercentileChart data={percentileSeries} percentile={percentile} />
            <TraceCountChart data={traceCountSeries} />
            <AvgDurationChart data={avgDurationSeries} />
            <ErrorCountChart data={errorCountSeries} />
          </Box>

          {/* Slowest traces */}
          <Typography variant="h6" gutterBottom fontWeight={600}>Top 20 Slowest Traces</Typography>
          <TableContainer component={Paper}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Name</TableCell>
                  <TableCell>Service</TableCell>
                  <TableCell>Duration</TableCell>
                  <TableCell>Start Time</TableCell>
                  <TableCell>Trace ID</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {slowTraces.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} align="center">No data</TableCell>
                  </TableRow>
                ) : slowTraces.map(t => (
                  <TableRow key={t.trace_id} onClick={() => setSelectedTraceId(t.trace_id)} sx={{ cursor: 'pointer', '&:hover': { background: 'rgba(0,0,0,0.04)' } }}>
                    <TableCell>{t.name}</TableCell>
                    <TableCell>
                      <Chip label={t.service} size="small" sx={{ fontFamily: 'monospace', fontSize: 11 }} />
                    </TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', color: t.duration_ms > 1000 ? '#ef4444' : t.duration_ms > 500 ? '#f59e0b' : 'inherit' }}>
                      {fmtMs(t.duration_ms)}
                    </TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: 12 }}>
                      {new Date(t.start_time / 1e6).toLocaleString()}
                    </TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: 11, color: 'text.secondary' }}>
                      {t.trace_id}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </>
      )}

      <Drawer
        anchor="right"
        open={selectedTraceId !== null}
        onClose={() => setSelectedTraceId(null)}
        PaperProps={{ sx: { width: '80vw', display: 'flex', flexDirection: 'column' } }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', px: 2, py: 1, borderBottom: 1, borderColor: 'divider', flexShrink: 0 }}>
          <Typography variant="subtitle1" sx={{ fontFamily: 'monospace', fontSize: '0.85rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {selectedTraceId}
          </Typography>
          <Box sx={{ display: 'flex', gap: 1, flexShrink: 0 }}>
            <Button size="small" startIcon={<OpenInNewIcon />} onClick={() => window.open(`/traces/${encodeURIComponent(selectedTraceId ?? '')}`, '_blank')}>
              Open in new tab
            </Button>
            <IconButton size="small" onClick={() => setSelectedTraceId(null)}>
              <CloseIcon />
            </IconButton>
          </Box>
        </Box>
        <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
          {selectedTraceId && <TraceDetails traceId={selectedTraceId} />}
        </Box>
      </Drawer>
    </Box>
  );
};
