import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Box, Typography, CircularProgress, Select, MenuItem, FormControl,
  InputLabel, Button, Table, TableBody, TableCell, TableContainer,
  TableHead, TableRow, Paper, Chip, TextField, Switch,
  TablePagination, TableSortLabel, Collapse, IconButton,
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import KeyboardArrowRightIcon from '@mui/icons-material/KeyboardArrowRight';
import { Link, useSearchParams, useNavigate, useLocation } from 'react-router-dom';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip as ReTooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts';
import { config } from '../config';

// ─── constants ────────────────────────────────────────────────────────────────

const TIME_PRESETS = [
  { label: 'Last 5m',   minutes: 5 },
  { label: 'Last 15m',  minutes: 15 },
  { label: 'Last 30m',  minutes: 30 },
  { label: 'Last 1h',   minutes: 60 },
  { label: 'Last 3h',   minutes: 180 },
  { label: 'Last 24h',  minutes: 1440 },
  { label: 'Custom',    minutes: 0 },
];

const REFRESH_INTERVALS = [
  { label: '10s', seconds: 10 },
  { label: '30s', seconds: 30 },
  { label: '1m',  seconds: 60 },
  { label: '5m',  seconds: 300 },
];

const SEVERITIES = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'];

const SEVERITY_COLORS: Record<string, 'default' | 'error' | 'warning' | 'info' | 'success'> = {
  TRACE: 'default',
  DEBUG: 'default',
  INFO: 'info',
  WARN: 'warning',
  ERROR: 'error',
  FATAL: 'error',
};

const SEVERITY_BAR_COLORS: Record<string, string> = {
  TRACE: '#9e9e9e',
  DEBUG: '#78909c',
  INFO:  '#42a5f5',
  WARN:  '#ffa726',
  ERROR: '#ef5350',
  FATAL: '#b71c1c',
};

// ─── types ────────────────────────────────────────────────────────────────────

interface LogVolumeBucket {
  bucket_ms: number;
  severity: string;
  count: number;
}

interface ChartBucket {
  ts: number;
  [sev: string]: number;
}

interface LogRow {
  timestamp_unix_nano: number;
  severity_text: string;
  severity_number: number;
  body: string;
  trace_id: string;
  span_id: string;
  service_name: string;
  scope_name: string;
  attributes: Record<string, string>;
  resource_attributes: Record<string, string>;
}

// ─── helpers ──────────────────────────────────────────────────────────────────

const SEVERITY_LABELS: Record<number, string> = { 1: 'TRACE', 5: 'DEBUG', 9: 'INFO', 13: 'WARN', 17: 'ERROR', 21: 'FATAL' };

function severityLabel(text: string, num: number): string {
  if (text) return text.toUpperCase();
  const base = Math.ceil(num / 4) * 4 - 3;
  return SEVERITY_LABELS[base] ?? String(num);
}

function fmtTs(ns: number): string {
  return new Date(ns / 1e6).toLocaleString();
}

function initParam(params: URLSearchParams, key: string, def: string): string {
  return params.get(key) ?? def;
}

// ─── component ────────────────────────────────────────────────────────────────

export const LogsPage: React.FC = () => {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const location = useLocation();

  const [timePresetIdx, setTimePresetIdx] = useState(() => {
    const v = searchParams.get('timePreset');
    return v ? parseInt(v) : 3; // default Last 1h
  });
  const [customStart, setCustomStart] = useState(() => initParam(searchParams, 'start', ''));
  const [customEnd, setCustomEnd] = useState(() => initParam(searchParams, 'end', ''));
  const [service, setService] = useState(() => initParam(searchParams, 'service', ''));
  const [severity, setSeverity] = useState(() => initParam(searchParams, 'severity', ''));
  const [body, setBody] = useState(() => initParam(searchParams, 'body', ''));
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [sortField, setSortField] = useState('timestamp');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [refreshIntervalIdx, setRefreshIntervalIdx] = useState(1);

  const [logs, setLogs] = useState<LogRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [chartData, setChartData] = useState<ChartBucket[]>([]);
  const [chartSeverities, setChartSeverities] = useState<string[]>([]);
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set());

  const silentFetchRef = useRef<(() => void) | null>(null);
  const refreshTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  function getDateRange() {
    const preset = TIME_PRESETS[timePresetIdx];
    if (preset.minutes > 0) {
      const end = new Date();
      const start = new Date(end.getTime() - preset.minutes * 60 * 1000);
      return { start: start.toISOString(), end: end.toISOString() };
    }
    return { start: customStart, end: customEnd };
  }

  const fetchLogs = useCallback((silent = false) => {
    const { start, end } = getDateRange();
    if (!start || !end) return;

    if (!silent) setLoading(true);
    setError(null);

    const baseParams = new URLSearchParams({ start, end });
    if (service) baseParams.set('service', service);
    if (severity) baseParams.set('severity', severity);
    if (body) baseParams.set('body', body);

    const logParams = new URLSearchParams(baseParams);
    logParams.set('page', String(page + 1));
    logParams.set('pageSize', String(pageSize));
    logParams.set('sort', sortField);
    logParams.set('sort_dir', sortDir);

    const logsReq = fetch(`${config.backendUrl}/api/logs?${logParams}`)
      .then(res => { if (!res.ok) throw new Error(`HTTP ${res.status}`); return res.json(); });

    const volReq = fetch(`${config.backendUrl}/api/logs/volume?${baseParams}`)
      .then(res => res.ok ? res.json() : []);

    Promise.all([logsReq, volReq])
      .then(([data, volData]: [LogRow[], LogVolumeBucket[]]) => {
        setLogs(data);
        setExpandedRows(new Set());

        const bucketMap = new Map<number, ChartBucket>();
        const sevSet = new Set<string>();
        for (const b of volData) {
          sevSet.add(b.severity);
          const existing = bucketMap.get(b.bucket_ms) ?? { ts: b.bucket_ms };
          existing[b.severity] = b.count;
          bucketMap.set(b.bucket_ms, existing);
        }
        const sorted = [...bucketMap.values()].sort((a, b) => a.ts - b.ts);
        setChartData(sorted);
        setChartSeverities([...sevSet].sort());
      })
      .catch(err => setError(err.message))
      .finally(() => { if (!silent) setLoading(false); });
  }, [timePresetIdx, customStart, customEnd, service, severity, body, page, pageSize, sortField, sortDir]); // eslint-disable-line react-hooks/exhaustive-deps

  // sync URL
  useEffect(() => {
    const p = new URLSearchParams();
    p.set('timePreset', String(timePresetIdx));
    if (customStart) p.set('start', customStart);
    if (customEnd) p.set('end', customEnd);
    if (service) p.set('service', service);
    if (severity) p.set('severity', severity);
    if (body) p.set('body', body);
    navigate({ search: p.toString() }, { replace: true });
  }, [timePresetIdx, customStart, customEnd, service, severity, body]); // eslint-disable-line react-hooks/exhaustive-deps

  // fetch on param change or back/forward nav
  useEffect(() => {
    fetchLogs();
  }, [location.key]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    fetchLogs();
  }, [sortField, sortDir, page, pageSize]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    silentFetchRef.current = () => fetchLogs(true);
  }, [fetchLogs]);

  // auto-refresh
  useEffect(() => {
    if (refreshTimerRef.current) clearInterval(refreshTimerRef.current);
    if (autoRefresh) {
      const secs = REFRESH_INTERVALS[refreshIntervalIdx].seconds;
      refreshTimerRef.current = setInterval(() => silentFetchRef.current?.(), secs * 1000);
    }
    return () => { if (refreshTimerRef.current) clearInterval(refreshTimerRef.current); };
  }, [autoRefresh, refreshIntervalIdx]);

  const isCustom = TIME_PRESETS[timePresetIdx].minutes === 0;

  function handleSort(field: string) {
    if (sortField === field) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDir('desc');
    }
    setPage(0);
  }

  return (
    <Box>
      <Typography variant="h5" gutterBottom>Logs</Typography>

      {/* ── controls ── */}
      <Box sx={{ display: 'flex', gap: 1.5, flexWrap: 'wrap', alignItems: 'center', mb: 2 }}>
        <FormControl size="small" sx={{ minWidth: 130 }}>
          <InputLabel>Time Range</InputLabel>
          <Select value={timePresetIdx} label="Time Range" onChange={e => { setTimePresetIdx(Number(e.target.value)); setPage(0); }}>
            {TIME_PRESETS.map((p, i) => <MenuItem key={i} value={i}>{p.label}</MenuItem>)}
          </Select>
        </FormControl>

        {isCustom && <>
          <TextField size="small" label="Start" type="datetime-local" value={customStart} onChange={e => setCustomStart(e.target.value)} InputLabelProps={{ shrink: true }} />
          <TextField size="small" label="End"   type="datetime-local" value={customEnd}   onChange={e => setCustomEnd(e.target.value)}   InputLabelProps={{ shrink: true }} />
        </>}

        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel>Severity</InputLabel>
          <Select value={severity} label="Severity" onChange={e => { setSeverity(e.target.value); setPage(0); }}>
            <MenuItem value="">All</MenuItem>
            {SEVERITIES.map(s => <MenuItem key={s} value={s}>{s}</MenuItem>)}
          </Select>
        </FormControl>

        <TextField size="small" label="Service" value={service} onChange={e => { setService(e.target.value); setPage(0); }} sx={{ minWidth: 140 }} />
        <TextField size="small" label="Body search" value={body} onChange={e => { setBody(e.target.value); setPage(0); }} sx={{ minWidth: 180 }}
          onKeyDown={e => { if (e.key === 'Enter') fetchLogs(); }}
        />

        <Button variant="contained" size="small" onClick={() => fetchLogs()} startIcon={<RefreshIcon />}>Refresh</Button>

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, ml: 'auto' }}>
          <Typography variant="body2" color="text.secondary">Auto-refresh</Typography>
          <Switch size="small" checked={autoRefresh} onChange={e => setAutoRefresh(e.target.checked)} />
          {autoRefresh && (
            <FormControl size="small" sx={{ minWidth: 80 }}>
              <Select value={refreshIntervalIdx} onChange={e => setRefreshIntervalIdx(Number(e.target.value))}>
                {REFRESH_INTERVALS.map((r, i) => <MenuItem key={i} value={i}>{r.label}</MenuItem>)}
              </Select>
            </FormControl>
          )}
        </Box>
      </Box>

      {/* ── volume chart ── */}
      {chartData.length > 0 && (
        <Paper variant="outlined" sx={{ p: 2, mb: 2 }}>
          <ResponsiveContainer width="100%" height={160}>
            <BarChart data={chartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis
                dataKey="ts"
                tickFormatter={v => new Date(v).toLocaleTimeString()}
                tick={{ fontSize: 11 }}
                minTickGap={40}
              />
              <YAxis tick={{ fontSize: 11 }} allowDecimals={false} width={36} />
              <ReTooltip
                labelFormatter={v => new Date(v).toLocaleString()}
                formatter={(value: number, name: string) => [value, name]}
              />
              {chartSeverities.map(sev => (
                <Bar key={sev} dataKey={sev} stackId="a"
                  fill={SEVERITY_BAR_COLORS[sev] ?? '#90a4ae'}
                  isAnimationActive={false}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </Paper>
      )}

      {/* ── table ── */}
      {loading ? (
        <Box display="flex" justifyContent="center" py={6}><CircularProgress /></Box>
      ) : error ? (
        <Typography color="error">{error}</Typography>
      ) : (
        <>
          <TableContainer component={Paper}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell padding="none" sx={{ width: 32 }} />
                  {([['timestamp', 'Timestamp'], ['severity', 'Severity'], ['service', 'Service'], ['body', 'Body']] as [string, string][]).map(([field, label]) => (
                    <TableCell key={field} sortDirection={sortField === field ? sortDir : false}>
                      <TableSortLabel
                        active={sortField === field}
                        direction={sortField === field ? sortDir : 'desc'}
                        onClick={() => handleSort(field)}
                      >
                        {label}
                      </TableSortLabel>
                    </TableCell>
                  ))}
                  <TableCell>Trace ID</TableCell>
                  <TableCell>Span ID</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {logs.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} align="center">
                      <Typography color="text.secondary" variant="body2" py={3}>No logs found</Typography>
                    </TableCell>
                  </TableRow>
                ) : logs.map((log, idx) => {
                  const sev = severityLabel(log.severity_text, log.severity_number);
                  const expanded = expandedRows.has(idx);
                  const toggle = () => setExpandedRows(prev => {
                    const next = new Set(prev);
                    next.has(idx) ? next.delete(idx) : next.add(idx);
                    return next;
                  });
                  return (
                    <React.Fragment key={idx}>
                      <TableRow hover onClick={toggle} sx={{ cursor: 'pointer' }}>
                        <TableCell padding="none" sx={{ width: 32, pl: 0.5 }}>
                          <IconButton size="small">
                            {expanded ? <KeyboardArrowDownIcon fontSize="small" /> : <KeyboardArrowRightIcon fontSize="small" />}
                          </IconButton>
                        </TableCell>
                        <TableCell sx={{ whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: 12 }}>
                          {fmtTs(log.timestamp_unix_nano)}
                        </TableCell>
                        <TableCell>
                          <Chip label={sev} size="small" color={SEVERITY_COLORS[sev] ?? 'default'} />
                        </TableCell>
                        <TableCell sx={{ fontSize: 12 }}>{log.service_name}</TableCell>
                        <TableCell sx={{ maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 12 }}>
                          {log.body}
                        </TableCell>
                        <TableCell sx={{ fontFamily: 'monospace', fontSize: 11 }}>
                          {log.trace_id ? (
                            <Link
                              to={`/traces/${log.trace_id}`}
                              onClick={e => e.stopPropagation()}
                              style={{ color: 'inherit', textDecoration: 'none' }}
                            >
                              <Box
                                component="span"
                                sx={{
                                  display: 'inline-flex', alignItems: 'center', gap: 0.5,
                                  '& .label': { display: 'inline' },
                                  '& .open': { display: 'none', fontSize: 11, fontFamily: 'sans-serif', color: 'primary.main' },
                                  '&:hover .label': { display: 'none' },
                                  '&:hover .open': { display: 'inline' },
                                }}
                              >
                                <span className="label">{log.trace_id.slice(0, 16)}…</span>
                                <span className="open">Open trace ↗</span>
                              </Box>
                            </Link>
                          ) : '—'}
                        </TableCell>
                        <TableCell sx={{ fontFamily: 'monospace', fontSize: 11 }}>
                          {log.span_id ? log.span_id.slice(0, 16) : '—'}
                        </TableCell>
                      </TableRow>
                      {expanded && (
                        <TableRow>
                          <TableCell colSpan={7} sx={{ py: 0, bgcolor: 'action.hover' }}>
                            <Collapse in={expanded} unmountOnExit>
                              <Box sx={{ p: 1.5, fontFamily: 'monospace', fontSize: 12, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                                {log.body}
                              </Box>
                            </Collapse>
                          </TableCell>
                        </TableRow>
                      )}
                    </React.Fragment>
                  );
                })}
              </TableBody>
            </Table>
          </TableContainer>
          <TablePagination
            component="div"
            count={-1}
            page={page}
            onPageChange={(_, p) => setPage(p)}
            rowsPerPage={pageSize}
            onRowsPerPageChange={e => { setPageSize(parseInt(e.target.value)); setPage(0); }}
            rowsPerPageOptions={[20, 50, 100]}
            labelDisplayedRows={({ from, to }) => `${from}–${to}`}
          />
        </>
      )}
    </Box>
  );
};
