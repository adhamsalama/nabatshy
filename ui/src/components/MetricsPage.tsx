import React, { useState, useEffect, useRef, useCallback, KeyboardEvent } from 'react';
import {
  Box, Typography, CircularProgress, Select, MenuItem, FormControl,
  InputLabel, Button, Card, CardContent, Switch, Table, TableBody,
  TableCell, TableContainer, TableHead, TableRow, Paper, Chip,
  TextField, Alert, Tooltip, InputAdornment, TablePagination,
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import { format } from 'date-fns';
import { useLocation, useNavigate } from 'react-router-dom';
import { config } from '../config.ts';
import MetricSeriesChart, { MetricSeries } from './MetricSeriesChart';
import { resolveUnit, formatMetricValue } from '../utils/metricUnits';

// ─── constants ───────────────────────────────────────────────────────────────

const TIME_PRESETS = [
  { label: 'Last 5m',   minutes: 5 },
  { label: 'Last 15m',  minutes: 15 },
  { label: 'Last 30m',  minutes: 30 },
  { label: 'Last 1h',   minutes: 60 },
  { label: 'Last 3h',   minutes: 180 },
  { label: 'Last 24h',  minutes: 1440 },
  { label: 'Last N...', minutes: -1 },
  { label: 'Custom',    minutes: 0 },
];

const REFRESH_INTERVALS = [
  { label: '10s', seconds: 10 },
  { label: '30s', seconds: 30 },
  { label: '1m',  seconds: 60 },
  { label: '5m',  seconds: 300 },
];

const TYPE_COLORS: Record<string, 'default' | 'primary' | 'success' | 'warning'> = {
  gauge: 'primary',
  sum: 'success',
  histogram: 'warning',
};

// ─── types ────────────────────────────────────────────────────────────────────

interface OtelMetricNameRow {
  metric_name: string;
  metric_type: string;
  metric_unit: string;
  scope_name: string;
  count: number;
  attribute_keys: string[];
}

interface OtelMetricRow {
  metric_name: string;
  metric_type: string;
  metric_unit: string;
  time_unix_nano: number;
  value_double: number;
  value_int: number;
  aggregation_temporality: string;
  is_monotonic: boolean;
  histogram_count: number;
  histogram_sum: number;
  scope_name: string;
  attributes: Record<string, string>;
  resource_attributes: Record<string, string>;
}

interface OtelMetricSeriesResponse {
  series: MetricSeries[];
  metric_type: string;
  metric_unit: string;
  bucket_interval: string;
}

interface FilterChip {
  key: string;
  value: string;
}

// ─── helpers ──────────────────────────────────────────────────────────────────

function formatValue(row: OtelMetricRow): string {
  if (row.metric_type === 'histogram') {
    return `count=${row.histogram_count.toLocaleString()} sum=${row.histogram_sum.toFixed(3)}`;
  }
  const v = row.value_double !== 0 ? row.value_double : row.value_int;
  return typeof v === 'number' ? v.toFixed(4) : String(v);
}


// ─── sub-components ──────────────────────────────────────────────────────────

interface StatCardProps { label: string; value: string; sub?: string; color?: string }
const StatCard: React.FC<StatCardProps> = ({ label, value, sub, color }) => (
  <Card sx={{ flex: 1, minWidth: 140 }}>
    <CardContent sx={{ pb: '12px !important' }}>
      <Typography variant="caption" color="text.secondary" display="block">{label}</Typography>
      <Typography variant="h5" sx={{ fontWeight: 700, color: color ?? 'text.primary', lineHeight: 1.2, mt: 0.5 }}>
        {value}
      </Typography>
      {sub && <Typography variant="caption" color="text.secondary">{sub}</Typography>}
    </CardContent>
  </Card>
);

function AttrChips({ attrs }: { attrs: Record<string, string> }) {
  const entries = Object.entries(attrs);
  if (entries.length === 0) return <Typography variant="caption" color="text.disabled">—</Typography>;
  return (
    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
      {entries.map(([k, v]) => (
        <Tooltip key={k} title={`${k}=${v}`}>
          <Chip label={`${k}=${v}`} size="small" variant="outlined" sx={{ maxWidth: 200, fontSize: '0.7rem' }} />
        </Tooltip>
      ))}
    </Box>
  );
}

// ─── main component ───────────────────────────────────────────────────────────

export const MetricsPage: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();

  const initParam = (key: string, fallback: string) =>
    new URLSearchParams(window.location.search).get(key) ?? fallback;

  // time range
  const [timePresetIdx, setTimePresetIdx] = useState(() => {
    const label = initParam('timePreset', 'Last 1h');
    const idx = TIME_PRESETS.findIndex(p => p.label === label);
    return idx >= 0 ? idx : 3;
  });
  const [customStart, setCustomStart] = useState(() => {
    const s = initParam('start', '');
    return s ? new Date(s) : new Date(Date.now() - 60 * 60 * 1000);
  });
  const [customEnd, setCustomEnd] = useState(() => {
    const e = initParam('end', '');
    return e ? new Date(e) : new Date();
  });
  const [customNMinutes, setCustomNMinutes] = useState(() => initParam('customMinutes', '60'));
  const isCustom = TIME_PRESETS[timePresetIdx].label === 'Custom';
  const isLastN  = TIME_PRESETS[timePresetIdx].label === 'Last N...';

  // auto-refresh
  const [autoRefresh, setAutoRefresh] = useState(() => initParam('autoRefresh', 'false') === 'true');
  const [refreshInterval, setRefreshInterval] = useState(() => parseInt(initParam('refreshInterval', '30')));

  // selectors
  const [selectedMetric, setSelectedMetric] = useState(() => initParam('metricName', ''));
  const [groupBy, setGroupBy] = useState(() => initParam('groupBy', ''));

  // filter chips
  const [filterChips, setFilterChips] = useState<FilterChip[]>(() => {
    try { return JSON.parse(initParam('filters', '[]')); } catch { return []; }
  });
  const [filterInput, setFilterInput] = useState('');

  // pagination
  const [page, setPage] = useState(() => parseInt(initParam('page', '0')));
  const [pageSize, setPageSize] = useState(() => parseInt(initParam('pageSize', '50')));

  // data
  const [names, setNames] = useState<OtelMetricNameRow[]>([]);
  const [seriesData, setSeriesData] = useState<OtelMetricSeriesResponse | null>(null);
  const [rawRows, setRawRows] = useState<OtelMetricRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ── helpers ──────────────────────────────────────────────────────────────

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

  const buildUrlParams = useCallback((overrides: {
    timePresetIdx?: number; customStart?: Date; customEnd?: Date; customNMinutes?: string;
    autoRefresh?: boolean; refreshInterval?: number;
    metricName?: string; groupBy?: string;
    filters?: FilterChip[]; page?: number; pageSize?: number;
  } = {}) => {
    const idx = overrides.timePresetIdx ?? timePresetIdx;
    const preset = TIME_PRESETS[idx];
    const params: Record<string, string> = {
      timePreset: preset.label,
      autoRefresh: String(overrides.autoRefresh ?? autoRefresh),
      refreshInterval: String(overrides.refreshInterval ?? refreshInterval),
      metricName: overrides.metricName ?? selectedMetric,
      groupBy: overrides.groupBy ?? groupBy,
      filters: JSON.stringify(overrides.filters ?? filterChips),
      page: String(overrides.page ?? page),
      pageSize: String(overrides.pageSize ?? pageSize),
    };
    if (preset.label === 'Custom') {
      params.start = (overrides.customStart ?? customStart).toISOString();
      params.end   = (overrides.customEnd ?? customEnd).toISOString();
    }
    if (preset.label === 'Last N...') {
      params.customMinutes = overrides.customNMinutes ?? customNMinutes;
    }
    return params;
  }, [timePresetIdx, autoRefresh, refreshInterval, selectedMetric, groupBy, filterChips, page, pageSize, customStart, customEnd, customNMinutes]);

  const pushUrl = useCallback((overrides: Parameters<typeof buildUrlParams>[0] = {}) => {
    navigate(`?${new URLSearchParams(buildUrlParams(overrides))}`, { replace: false, state: { internal: true } });
  }, [navigate, buildUrlParams]);

  // ── fetch names (once on mount) ──────────────────────────────────────────

  useEffect(() => {
    fetch(`${config.backendUrl}/api/otel-metrics/names`)
      .then(r => r.ok ? r.json() : [])
      .then(d => setNames(d ?? []))
      .catch(() => {});
  }, []);

  // ── fetch series + raw data ───────────────────────────────────────────────

  const fetchData = useCallback(async (overrideStart?: Date, overrideEnd?: Date, silent = false) => {
    if (!selectedMetric) { setSeriesData(null); setRawRows([]); return; }

    const { start: rs, end: re } = getDateRange();
    const start = overrideStart ?? rs;
    const end   = overrideEnd   ?? re;
    if (isNaN(start.getTime()) || isNaN(end.getTime())) { setError('Invalid date range'); return; }

    if (!silent) setLoading(true);
    setError(null);
    try {
      const base = new URLSearchParams({
        metric_name: selectedMetric,
        start: start.toISOString(),
        end: end.toISOString(),
      });
      if (groupBy) base.set('group_by', groupBy);

      const rawParams = new URLSearchParams(base);
      rawParams.set('limit', '500');

      const [sRes, rRes] = await Promise.all([
        fetch(`${config.backendUrl}/api/otel-metrics/series?${base}`),
        fetch(`${config.backendUrl}/api/otel-metrics?${rawParams}`),
      ]);

      if (sRes.ok) setSeriesData(await sRes.json());
      else setSeriesData(null);

      if (rRes.ok) setRawRows(await rRes.json() ?? []);
      else setRawRows([]);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'An error occurred');
    } finally {
      if (!silent) setLoading(false);
    }
  }, [selectedMetric, groupBy, getDateRange]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const silentFetchRef = useRef(() => {});
  useEffect(() => { silentFetchRef.current = () => fetchData(undefined, undefined, true); });
  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(() => silentFetchRef.current(), refreshInterval * 1000);
    return () => clearInterval(id);
  }, [autoRefresh, refreshInterval]);

  // ── restore state from URL on browser nav ────────────────────────────────

  useEffect(() => {
    if ((location.state as { internal?: boolean })?.internal) return;
    const p = new URLSearchParams(location.search);
    const label = p.get('timePreset') ?? 'Last 1h';
    const idx = TIME_PRESETS.findIndex(t => t.label === label);
    setTimePresetIdx(idx >= 0 ? idx : 3);
    setAutoRefresh(p.get('autoRefresh') === 'true');
    setRefreshInterval(parseInt(p.get('refreshInterval') || '30'));
    setSelectedMetric(p.get('metricName') ?? '');
    setGroupBy(p.get('groupBy') ?? '');
    setPage(parseInt(p.get('page') || '0'));
    setPageSize(parseInt(p.get('pageSize') || '50'));
    try { setFilterChips(JSON.parse(p.get('filters') || '[]')); } catch { setFilterChips([]); }
    if (idx >= 0 && TIME_PRESETS[idx].label === 'Custom') {
      const s = p.get('start'); const e = p.get('end');
      if (s) setCustomStart(new Date(s));
      if (e) setCustomEnd(new Date(e));
    }
    if (idx >= 0 && TIME_PRESETS[idx].label === 'Last N...') {
      const n = p.get('customMinutes'); if (n) setCustomNMinutes(n);
    }
  }, [location.key]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── chart range select ────────────────────────────────────────────────────

  const handleChartRangeSelect = useCallback((startStr: string, endStr: string) => {
    const s = new Date(startStr);
    const e = new Date(endStr);
    const customIdx = TIME_PRESETS.findIndex(p => p.label === 'Custom');
    setCustomStart(s);
    setCustomEnd(e);
    setTimePresetIdx(customIdx);
    pushUrl({ customStart: s, customEnd: e, timePresetIdx: customIdx });
    fetchData(s, e);
  }, [pushUrl]);

  // ── filter chips ──────────────────────────────────────────────────────────

  const commitFilterInput = useCallback(() => {
    const raw = filterInput.trim();
    if (!raw) return;
    const eq = raw.indexOf('=');
    const chip: FilterChip = eq > 0
      ? { key: raw.slice(0, eq).trim(), value: raw.slice(eq + 1).trim() }
      : { key: raw, value: '' };
    const next = [...filterChips, chip];
    setFilterChips(next);
    setFilterInput('');
    pushUrl({ filters: next });
  }, [filterInput, filterChips, pushUrl]);

  const removeFilterChip = useCallback((i: number) => {
    const next = filterChips.filter((_, j) => j !== i);
    setFilterChips(next);
    pushUrl({ filters: next });
  }, [filterChips, pushUrl]);

  const handleFilterKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); commitFilterInput(); }
    if (e.key === 'Backspace' && filterInput === '' && filterChips.length > 0) {
      removeFilterChip(filterChips.length - 1);
    }
  };

  // ── derived data ──────────────────────────────────────────────────────────

  const selectedMetricInfo = names.find(n => n.metric_name === selectedMetric);
  const availableGroupBys  = selectedMetricInfo?.attribute_keys ?? [];
  const metricType = seriesData?.metric_type ?? selectedMetricInfo?.metric_type ?? '';
  const metricUnit = seriesData?.metric_unit ?? selectedMetricInfo?.metric_unit ?? '';

  const filteredRows = filterChips.length === 0 ? rawRows : rawRows.filter(row =>
    filterChips.every(chip =>
      row.attributes[chip.key] === chip.value || row.resource_attributes[chip.key] === chip.value
    )
  );
  const pagedRows = filteredRows.slice(page * pageSize, page * pageSize + pageSize);

  // stat cards
  const allPoints = (seriesData?.series ?? []).flatMap(s => s.points);
  const vals = allPoints.map(p => p.value).filter(v => isFinite(v));
  const latestVal  = vals.length > 0 ? vals[vals.length - 1] : null;
  const minVal     = vals.length > 0 ? Math.min(...vals) : null;
  const maxVal     = vals.length > 0 ? Math.max(...vals) : null;
  const avgVal     = vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
  const totalCount = allPoints.reduce((a, p) => a + (p.histogram_count ?? 0), 0);
  const totalSum   = allPoints.reduce((a, p) => a + (p.histogram_sum ?? 0), 0);

  const resolvedMetricUnit = resolveUnit(selectedMetric, metricUnit);
  const fmt = (v: number | null) => v !== null ? formatMetricValue(v, resolvedMetricUnit) : '—';

  // ── render ────────────────────────────────────────────────────────────────

  return (
    <Box sx={{ p: 3 }}>
      {/* ── page title ── */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 3 }}>
        {selectedMetric && (
          <Button
            size="small"
            startIcon={<ArrowBackIcon />}
            onClick={() => { setSelectedMetric(''); setGroupBy(''); pushUrl({ metricName: '', groupBy: '' }); }}
          >
            All Metrics
          </Button>
        )}
        <Typography variant="h5" fontWeight={600}>
          {selectedMetric || 'Metrics'}
        </Typography>
        {selectedMetricInfo && (
          <Chip label={selectedMetricInfo.metric_type} size="small" color={TYPE_COLORS[selectedMetricInfo.metric_type] ?? 'default'} />
        )}
        {metricUnit && <Typography variant="body2" color="text.secondary">({metricUnit})</Typography>}
      </Box>

      {/* ── toolbar ── */}
      <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center', mb: 2 }}>
        {/* time range */}
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
              label="Start" type="datetime-local" size="small"
              value={format(customStart, "yyyy-MM-dd'T'HH:mm:ss")}
              onChange={e => { const s = new Date(e.target.value); setCustomStart(s); pushUrl({ customStart: s }); }}
              InputLabelProps={{ shrink: true }} inputProps={{ step: 1 }}
            />
            <TextField
              label="End" type="datetime-local" size="small"
              value={format(customEnd, "yyyy-MM-dd'T'HH:mm:ss")}
              onChange={e => { const d = new Date(e.target.value); setCustomEnd(d); pushUrl({ customEnd: d }); }}
              InputLabelProps={{ shrink: true }} inputProps={{ step: 1 }}
            />
          </>
        )}

        {isLastN && (
          <TextField
            label="Minutes" type="number" size="small" value={customNMinutes}
            onChange={e => { setCustomNMinutes(e.target.value); pushUrl({ customNMinutes: e.target.value }); }}
            onBlur={() => fetchData()} onKeyDown={e => { if (e.key === 'Enter') fetchData(); }}
            InputLabelProps={{ shrink: true }} inputProps={{ min: 1 }} sx={{ width: 110 }}
          />
        )}

        {/* metric selector */}
        <FormControl size="small" sx={{ minWidth: 240 }}>
          <InputLabel shrink>Metric</InputLabel>
          <Select
            value={selectedMetric} label="Metric"
            onChange={e => {
              const m = e.target.value as string;
              setSelectedMetric(m); setGroupBy('');
              pushUrl({ metricName: m, groupBy: '' });
            }}
            displayEmpty
            renderValue={(v: string) => v || 'All Metrics'}
            notched
          >
            <MenuItem value="">All Metrics</MenuItem>
            {names.map(n => (
              <MenuItem key={n.metric_name} value={n.metric_name}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                  <span style={{ flex: 1, fontFamily: 'monospace', fontSize: '0.8rem' }}>{n.metric_name}</span>
                  <Chip label={n.metric_type} size="small" color={TYPE_COLORS[n.metric_type] ?? 'default'} />
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>

        {/* group by — only when metric selected and has attributes */}
        {selectedMetric && availableGroupBys.length > 0 && (
          <FormControl size="small" sx={{ minWidth: 160 }}>
            <InputLabel shrink>Group by</InputLabel>
            <Select
              value={groupBy} label="Group by"
              onChange={e => { const g = e.target.value as string; setGroupBy(g); pushUrl({ groupBy: g }); }}
              displayEmpty renderValue={(v: string) => v || 'None'} notched
            >
              <MenuItem value="">None</MenuItem>
              {availableGroupBys.map(k => <MenuItem key={k} value={k}>{k}</MenuItem>)}
            </Select>
          </FormControl>
        )}

        {/* auto-refresh */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography variant="body2">Auto-refresh</Typography>
          <Switch checked={autoRefresh} size="small" onChange={e => {
            const v = e.target.checked; setAutoRefresh(v); pushUrl({ autoRefresh: v });
          }} />
        </Box>

        {autoRefresh && (
          <FormControl size="small" sx={{ minWidth: 100 }}>
            <InputLabel>Interval</InputLabel>
            <Select value={refreshInterval} label="Interval" onChange={e => {
              const v = Number(e.target.value); setRefreshInterval(v); pushUrl({ refreshInterval: v });
            }}>
              {REFRESH_INTERVALS.map(r => <MenuItem key={r.seconds} value={r.seconds}>{r.label}</MenuItem>)}
            </Select>
          </FormControl>
        )}

        <Button variant="outlined" size="small" startIcon={<RefreshIcon />} onClick={() => fetchData()} disabled={loading}>
          Refresh
        </Button>
      </Box>

      {/* ── attribute filter chips ── */}
      {selectedMetric && (
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, alignItems: 'center', mb: 2 }}>
          {filterChips.map((chip, i) => (
            <Chip
              key={i}
              label={chip.value ? `${chip.key}=${chip.value}` : chip.key}
              size="small"
              onDelete={() => removeFilterChip(i)}
              color="primary"
              variant="outlined"
            />
          ))}
          <TextField
            size="small"
            placeholder="Filter key=value…"
            value={filterInput}
            onChange={e => setFilterInput(e.target.value)}
            onKeyDown={handleFilterKey}
            onBlur={commitFilterInput}
            InputProps={{
              startAdornment: filterChips.length === 0
                ? <InputAdornment position="start"><Typography variant="caption" color="text.secondary">filter:</Typography></InputAdornment>
                : undefined,
            }}
            sx={{ width: 220 }}
          />
        </Box>
      )}

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      {loading && <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}><CircularProgress /></Box>}

      {/* ── OVERVIEW — no metric selected ── */}
      {!loading && !selectedMetric && (
        <>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            {names.length} unique metrics · click a row to inspect
          </Typography>
          <TableContainer component={Paper}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Metric Name</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell>Unit</TableCell>
                  <TableCell>Scope</TableCell>
                  <TableCell align="right">Data Points</TableCell>
                  <TableCell>Attribute Keys</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {names.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={6} align="center">
                      <Typography variant="body2" color="text.secondary" py={4}>
                        No metrics received yet — make sure the test server is running
                      </Typography>
                    </TableCell>
                  </TableRow>
                )}
                {names.map(n => (
                  <TableRow
                    key={n.metric_name}
                    hover
                    sx={{ cursor: 'pointer' }}
                    onClick={() => { setSelectedMetric(n.metric_name); setGroupBy(''); pushUrl({ metricName: n.metric_name, groupBy: '' }); }}
                  >
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{n.metric_name}</TableCell>
                    <TableCell>
                      <Chip label={n.metric_type} size="small" color={TYPE_COLORS[n.metric_type] ?? 'default'} />
                    </TableCell>
                    <TableCell sx={{ color: 'text.secondary', fontSize: '0.8rem' }}>{n.metric_unit || '—'}</TableCell>
                    <TableCell sx={{ color: 'text.secondary', fontSize: '0.8rem' }}>{n.scope_name || '—'}</TableCell>
                    <TableCell align="right" sx={{ fontFamily: 'monospace' }}>{n.count.toLocaleString()}</TableCell>
                    <TableCell>
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                        {(n.attribute_keys ?? []).map(k => (
                          <Chip key={k} label={k} size="small" variant="outlined" sx={{ fontSize: '0.7rem' }} />
                        ))}
                      </Box>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </>
      )}

      {/* ── DETAIL — metric selected ── */}
      {!loading && selectedMetric && (
        <>
          {/* stat cards */}
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', mb: 3 }}>
            {metricType === 'histogram' ? (
              <>
                <StatCard label="Total Count"   value={totalCount.toLocaleString()} sub={TIME_PRESETS[timePresetIdx].label} />
                <StatCard label="Total Sum"      value={fmt(totalSum)} sub={metricUnit || undefined} />
                <StatCard label="Avg Value"      value={totalCount > 0 ? fmt(totalSum / totalCount) : '—'} />
                <StatCard label="Buckets"        value={allPoints.length.toLocaleString()} sub="time buckets" />
              </>
            ) : (
              <>
                <StatCard label="Latest"  value={fmt(latestVal)} />
                <StatCard label="Min"     value={fmt(minVal)} />
                <StatCard label="Max"     value={fmt(maxVal)} />
                <StatCard label="Avg"     value={fmt(avgVal)} />
              </>
            )}
          </Box>

          {/* time series chart */}
          {seriesData && seriesData.series.length > 0 ? (
            <Box sx={{ mb: 3 }}>
              <MetricSeriesChart
                series={seriesData.series}
                unit={metricUnit}
                metricType={metricType}
                title={selectedMetric}
                metricName={selectedMetric}
                onRangeSelect={handleChartRangeSelect}
              />
              <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
                Bucket interval: {seriesData.bucket_interval}
                {metricType === 'histogram' ? ' · showing avg value per bucket (sum/count)' : ''}
              </Typography>
            </Box>
          ) : (
            !loading && <Alert severity="info" sx={{ mb: 3 }}>No series data for the selected time range.</Alert>
          )}

          {/* raw data table */}
          <Typography variant="h6" fontWeight={600} gutterBottom>
            Raw Data Points
            <Typography component="span" variant="body2" color="text.secondary" sx={{ ml: 1 }}>
              ({filteredRows.length.toLocaleString()} rows{filterChips.length > 0 ? ', filtered' : ''})
            </Typography>
          </Typography>
          <TableContainer component={Paper}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Time</TableCell>
                  <TableCell>Value</TableCell>
                  <TableCell>Scope</TableCell>
                  <TableCell>Attributes</TableCell>
                  <TableCell>Resource</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {pagedRows.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={5} align="center">
                      <Typography variant="body2" color="text.secondary" py={3}>No data</Typography>
                    </TableCell>
                  </TableRow>
                )}
                {pagedRows.map((row, i) => (
                  <TableRow key={i} hover>
                    <TableCell sx={{ whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: '0.75rem' }}>
                      {new Date(row.time_unix_nano / 1_000_000).toLocaleString()}
                    </TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8rem', whiteSpace: 'nowrap' }}>
                      {formatValue(row)}
                    </TableCell>
                    <TableCell sx={{ fontSize: '0.75rem', color: 'text.secondary' }}>{row.scope_name || '—'}</TableCell>
                    <TableCell><AttrChips attrs={row.attributes} /></TableCell>
                    <TableCell><AttrChips attrs={row.resource_attributes} /></TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <TablePagination
              component="div"
              count={filteredRows.length}
              page={page}
              onPageChange={(_, p) => { setPage(p); pushUrl({ page: p }); }}
              rowsPerPage={pageSize}
              onRowsPerPageChange={e => {
                const s = parseInt(e.target.value);
                setPageSize(s); setPage(0); pushUrl({ pageSize: s, page: 0 });
              }}
              rowsPerPageOptions={[20, 50, 100]}
            />
          </TableContainer>
        </>
      )}
    </Box>
  );
};
