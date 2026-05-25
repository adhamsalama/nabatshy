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
  TextField,
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import { format } from 'date-fns';
import CloseIcon from '@mui/icons-material/Close';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import { useLocation, useNavigate } from 'react-router-dom';
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
  { label: 'Last N...',minutes: -1 },
  { label: 'Custom',   minutes: 0 },
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
  const location = useLocation();
  const navigate = useNavigate();

  const initParam = (key: string, fallback: string) =>
    new URLSearchParams(window.location.search).get(key) ?? fallback;

  const [percentileSeries, setPercentileSeries] = useState<TimePercentile[]>([]);
  const [traceCountSeries, setTraceCountSeries] = useState<TimePercentile[]>([]);
  const [avgDurationSeries, setAvgDurationSeries] = useState<TimePercentile[]>([]);
  const [errorCountSeries, setErrorCountSeries] = useState<TimePercentile[]>([]);
  const [slowTraces, setSlowTraces] = useState<SlowTrace[]>([]);
  const [selectedTraceId, setSelectedTraceId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [percentile, setPercentile] = useState<number>(() => parseInt(initParam('percentile', '95')));
  const [timePresetIdx, setTimePresetIdx] = useState(() => {
    const label = initParam('timePreset', 'Last 1h');
    const idx = TIME_PRESETS.findIndex(p => p.label === label);
    return idx >= 0 ? idx : 3;
  });
  const [selectedService, setSelectedService] = useState<string>(() => initParam('service', ''));
  const [availableServices, setAvailableServices] = useState<string[]>([]);
  const [traceOrSpan, setTraceOrSpan] = useState<'trace' | 'span'>(() =>
    initParam('traceOrSpan', 'trace') === 'span' ? 'span' : 'trace'
  );
  const [autoRefresh, setAutoRefresh] = useState(() => initParam('autoRefresh', 'false') === 'true');
  const [refreshInterval, setRefreshInterval] = useState(() => parseInt(initParam('refreshInterval', '30')));
  const [customStart, setCustomStart] = useState(() => {
    const s = initParam('start', '');
    return s ? new Date(s) : new Date(Date.now() - 60 * 60 * 1000);
  });
  const [customEnd, setCustomEnd] = useState(() => {
    const e = initParam('end', '');
    return e ? new Date(e) : new Date();
  });
  const [customNMinutes, setCustomNMinutes] = useState(() => initParam('customNMinutes', '60'));
  const isCustom = TIME_PRESETS[timePresetIdx].label === 'Custom';
  const isLastN = TIME_PRESETS[timePresetIdx].label === 'Last N...';

  const buildUrlParams = useCallback((overrides: {
    timePresetIdx?: number; percentile?: number; service?: string;
    traceOrSpan?: string; autoRefresh?: boolean; refreshInterval?: number;
    customStart?: Date; customEnd?: Date; customNMinutes?: string;
  } = {}) => {
    const idx = overrides.timePresetIdx ?? timePresetIdx;
    const preset = TIME_PRESETS[idx];
    const params: Record<string, string> = {
      timePreset: preset.label,
      percentile: String(overrides.percentile ?? percentile),
      traceOrSpan: overrides.traceOrSpan ?? traceOrSpan,
      autoRefresh: String(overrides.autoRefresh ?? autoRefresh),
      refreshInterval: String(overrides.refreshInterval ?? refreshInterval),
    };
    const svc = overrides.service !== undefined ? overrides.service : selectedService;
    if (svc) params.service = svc;
    if (preset.label === 'Custom') {
      params.start = (overrides.customStart ?? customStart).toISOString();
      params.end = (overrides.customEnd ?? customEnd).toISOString();
    }
    if (preset.label === 'Last N...') {
      params.customNMinutes = overrides.customNMinutes ?? customNMinutes;
    }
    return params;
  }, [timePresetIdx, percentile, traceOrSpan, autoRefresh, refreshInterval, selectedService, customStart, customEnd, customNMinutes]);

  const pushUrl = useCallback((overrides: Parameters<typeof buildUrlParams>[0] = {}) => {
    navigate(`?${new URLSearchParams(buildUrlParams(overrides))}`, { replace: false, state: { internal: true } });
  }, [navigate, buildUrlParams]);

  const getDateRange = useCallback(() => {
    if (isCustom) return { start: customStart, end: customEnd };
    const end = new Date();
    if (isLastN) {
      const mins = parseInt(customNMinutes) || 60;
      return { start: new Date(end.getTime() - mins * 60 * 1000), end };
    }
    const start = new Date(end.getTime() - TIME_PRESETS[timePresetIdx].minutes * 60 * 1000);
    return { start, end };
  }, [timePresetIdx, isCustom, isLastN, customStart, customEnd, customNMinutes]);

  const fetchMetrics = useCallback(async (overrideStart?: Date, overrideEnd?: Date) => {
    const { start: rangeStart, end: rangeEnd } = getDateRange();
    const start = overrideStart ?? rangeStart;
    const end = overrideEnd ?? rangeEnd;
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
      slowUrl.searchParams.set('start', start.toISOString());
      slowUrl.searchParams.set('end', end.toISOString());
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

  // Restore state from URL on back/forward navigation
  useEffect(() => {
    if ((location.state as { internal?: boolean })?.internal) return;
    const p = new URLSearchParams(location.search);
    const label = p.get('timePreset') ?? 'Last 1h';
    const idx = TIME_PRESETS.findIndex(t => t.label === label);
    const newIdx = idx >= 0 ? idx : 3;
    const newPercentile = parseInt(p.get('percentile') || '95');
    const newService = p.get('service') ?? '';
    const newTOS = (p.get('traceOrSpan') === 'span' ? 'span' : 'trace') as 'trace' | 'span';
    const newAutoRefresh = p.get('autoRefresh') === 'true';
    const newInterval = parseInt(p.get('refreshInterval') || '30');
    setTimePresetIdx(newIdx);
    setPercentile(newPercentile);
    setSelectedService(newService);
    setTraceOrSpan(newTOS);
    setAutoRefresh(newAutoRefresh);
    setRefreshInterval(newInterval);
    if (TIME_PRESETS[newIdx].label === 'Custom') {
      const s = p.get('start'); const e = p.get('end');
      if (s) setCustomStart(new Date(s));
      if (e) setCustomEnd(new Date(e));
    }
    if (TIME_PRESETS[newIdx].label === 'Last N...') {
      const n = p.get('customNMinutes');
      if (n) setCustomNMinutes(n);
    }
  }, [location.key]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleChartRangeSelect = useCallback((startStr: string, endStr: string) => {
    const s = new Date(startStr);
    const e = new Date(endStr);
    const customIdx = TIME_PRESETS.findIndex(p => p.label === 'Custom');
    setCustomStart(s);
    setCustomEnd(e);
    setTimePresetIdx(customIdx);
    pushUrl({ customStart: s, customEnd: e, timePresetIdx: customIdx });
    fetchRef.current(s, e);
  }, [pushUrl]);

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
          <Select value={timePresetIdx} label="Time Range" onChange={e => {
            const idx = Number(e.target.value);
            setTimePresetIdx(idx);
            pushUrl({ timePresetIdx: idx });
          }}>
            {TIME_PRESETS.map((p, i) => <MenuItem key={i} value={i}>{p.label}</MenuItem>)}
          </Select>
        </FormControl>

        {isCustom && (
          <>
            <TextField
              label="Start Time"
              type="datetime-local"
              size="small"
              value={format(customStart, "yyyy-MM-dd'T'HH:mm:ss")}
              onChange={e => { const s = new Date(e.target.value); setCustomStart(s); pushUrl({ customStart: s }); }}
              InputLabelProps={{ shrink: true }}
              inputProps={{ step: 1 }}
            />
            <TextField
              label="End Time"
              type="datetime-local"
              size="small"
              value={format(customEnd, "yyyy-MM-dd'T'HH:mm:ss")}
              onChange={e => { const d = new Date(e.target.value); setCustomEnd(d); pushUrl({ customEnd: d }); }}
              InputLabelProps={{ shrink: true }}
              inputProps={{ step: 1 }}
            />
          </>
        )}

        {isLastN && (
          <TextField
            label="Minutes"
            type="number"
            size="small"
            value={customNMinutes}
            onChange={e => {
              const v = e.target.value;
              setCustomNMinutes(v);
              pushUrl({ customNMinutes: v });
            }}
            onBlur={() => fetchMetrics()}
            onKeyDown={e => { if (e.key === 'Enter') fetchMetrics(); }}
            InputLabelProps={{ shrink: true }}
            inputProps={{ min: 1 }}
            sx={{ width: 110 }}
          />
        )}

        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel>Percentile</InputLabel>
          <Select value={percentile} label="Percentile" onChange={e => {
            const p = Number(e.target.value);
            setPercentile(p);
            pushUrl({ percentile: p });
          }}>
            {percentileOptions.map(p => <MenuItem key={p} value={p}>P{p}</MenuItem>)}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 180 }}>
          <InputLabel shrink>Service</InputLabel>
          <Select value={selectedService} label="Service" onChange={e => {
            const svc = e.target.value;
            setSelectedService(svc);
            pushUrl({ service: svc });
          }} displayEmpty renderValue={(v: string) => v || 'All Services'} notched>
            <MenuItem value="">All Services</MenuItem>
            {availableServices.map(s => <MenuItem key={s} value={s}>{s}</MenuItem>)}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 130 }}>
          <InputLabel>Type</InputLabel>
          <Select value={traceOrSpan} label="Type" onChange={e => {
            const v = e.target.value as 'trace' | 'span';
            setTraceOrSpan(v);
            pushUrl({ traceOrSpan: v });
          }}>
            <MenuItem value="trace">Trace</MenuItem>
            <MenuItem value="span">Span</MenuItem>
          </Select>
        </FormControl>

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography variant="body2">Auto-refresh</Typography>
          <Switch checked={autoRefresh} onChange={e => {
            const v = e.target.checked;
            setAutoRefresh(v);
            pushUrl({ autoRefresh: v });
          }} size="small" />
        </Box>

        {autoRefresh && (
          <FormControl size="small" sx={{ minWidth: 100 }}>
            <InputLabel>Interval</InputLabel>
            <Select value={refreshInterval} label="Interval" onChange={e => {
              const v = Number(e.target.value);
              setRefreshInterval(v);
              pushUrl({ refreshInterval: v });
            }}>
              {REFRESH_INTERVALS.map(r => <MenuItem key={r.seconds} value={r.seconds}>{r.label}</MenuItem>)}
            </Select>
          </FormControl>
        )}

        <Button variant="outlined" startIcon={<RefreshIcon />} onClick={() => fetchMetrics()} disabled={loading}>
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
            <PercentileChart data={percentileSeries} percentile={percentile} onRangeSelect={handleChartRangeSelect} />
            <TraceCountChart data={traceCountSeries} onRangeSelect={handleChartRangeSelect} />
            <AvgDurationChart data={avgDurationSeries} onRangeSelect={handleChartRangeSelect} />
            <ErrorCountChart data={errorCountSeries} onRangeSelect={handleChartRangeSelect} />
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
