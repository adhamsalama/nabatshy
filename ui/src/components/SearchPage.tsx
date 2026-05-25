import React, { useState, useEffect, useRef } from 'react';
import {
  Box,
  TextField,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Typography,
  CircularProgress,
  IconButton,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  SelectChangeEvent,
  Button,
  Switch,
  Drawer,
  Popover,
  FormControlLabel,
  Checkbox,
  Divider,
  Chip,
  InputAdornment,
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import RefreshIcon from '@mui/icons-material/Refresh';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import CloseIcon from '@mui/icons-material/Close';
import ViewColumnIcon from '@mui/icons-material/ViewColumn';
import { format } from 'date-fns';
import { useSearchParams, useLocation, useNavigate } from 'react-router-dom';
import TraceCountChart from './TraceCountChart';
import { TraceDetails } from './TraceDetails';
import { TimePercentile } from './PercentileChart';
import { config } from "../config.ts";

interface SearchResult {
  TraceID: string;
  SpanID: string;
  Name: string;
  Service: string;
  Duration: number;
  StartTime: number;
  hasError: boolean;
  ResourceAttrs: Record<string, string>;
  SpanAttrs: Record<string, string>;
}

interface SearchResponse {
  results?: SearchResult[];
  page: number;
  pageSize: number;
}

const FilterChipInput = ({ value, onChange, onSearch }: { value: string; onChange: (v: string) => void; onSearch: (q: string) => void }) => {
  const [inputValue, setInputValue] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

  const chips = value.split(',').map(p => p.trim()).filter(Boolean);

  const commit = (andSearch = false) => {
    const trimmed = inputValue.trim().replace(/,$/, '');
    if (!trimmed) { if (andSearch) onSearch(value); return; }
    const newChips = [...chips, trimmed];
    const newQuery = newChips.join(',');
    onChange(newQuery);
    setInputValue('');
    if (andSearch) onSearch(newQuery);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') { commit(true); return; }
    if (e.key === ',') { e.preventDefault(); commit(true); return; }
    if (e.key === 'Backspace' && inputValue === '' && chips.length > 0) {
      const newChips = chips.slice(0, -1);
      const newQuery = newChips.join(',');
      onChange(newQuery);
      onSearch(newQuery);
    }
  };

  const removeChip = (idx: number) => {
    const newChips = chips.filter((_, i) => i !== idx);
    const newQuery = newChips.join(',');
    onChange(newQuery);
    onSearch(newQuery);
  };

  return (
    <Box
      onClick={() => inputRef.current?.focus()}
      sx={{
        display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 0.5,
        border: '1px solid', borderColor: 'divider', borderRadius: 1,
        px: 1, py: 0.75, cursor: 'text', flex: 1,
        '&:focus-within': { borderColor: 'primary.main', borderWidth: '2px', px: '7px', py: '5px' },
      }}
    >
      {chips.map((chip, i) => (
        <Chip key={i} label={chip} size="small" onDelete={() => removeChip(i)} sx={{ fontFamily: 'monospace', fontSize: 12 }} />
      ))}
      <Box
        component="input"
        ref={inputRef}
        value={inputValue}
        onChange={e => setInputValue(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={chips.length === 0 ? 'http.method!=GET, name=GetUser' : ''}
        sx={{
          border: 'none', outline: 'none', background: 'transparent',
          color: 'text.primary', fontFamily: 'monospace', fontSize: 13,
          flex: 1, minWidth: 160, py: 0.25,
        }}
      />
      <InputAdornment position="end">
        <IconButton size="small" onClick={() => commit(true)}>
          <SearchIcon />
        </IconButton>
      </InputAdornment>
    </Box>
  );
};

export const SearchPage: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const location = useLocation();
  const navigate = useNavigate();
  const [traceOrSpan, setTraceOrSpan] = useState<"trace" | "span" | "all">("trace");

  const [query, setQuery] = useState('');
  const [searchResponse, setSearchResponse] = useState<SearchResponse | null>(null);
  const [traceCountSeries, setTraceCountSeries] = useState<TimePercentile[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [sortField, setSortField] = useState<'start_time' | 'end_time' | 'duration'>('start_time');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [startDate, setStartDate] = useState(() => new Date(Date.now() - 5 * 60 * 1000));
  const [endDate, setEndDate] = useState(() => new Date());
  const [selectedService, setSelectedService] = useState<string>('');
  const [availableServices, setAvailableServices] = useState<string[]>([]);

  const [selectedTraceId, setSelectedTraceId] = useState<string | null>(null);
  const [selectedSpanId, setSelectedSpanId] = useState<string | null>(null);

  const DEFAULT_COLUMNS = ['traceId', 'spanId', 'name', 'service', 'duration', 'startTime', 'endTime'];
  const [visibleColumns, setVisibleColumns] = useState<Set<string>>(() => {
    const p = new URLSearchParams(window.location.search);
    const v = p.get('columns');
    return v ? new Set(v.split(',').filter(Boolean)) : new Set(DEFAULT_COLUMNS);
  });
  const [extraColumns, setExtraColumns] = useState<string[]>(() => {
    const p = new URLSearchParams(window.location.search);
    const v = p.get('extraColumns');
    return v ? v.split(',').filter(Boolean) : [];
  });
  const [columnAnchorEl, setColumnAnchorEl] = useState<HTMLElement | null>(null);
  const columnsButtonRef = useRef<HTMLButtonElement>(null);

  const toggleColumn = (id: string) => {
    setVisibleColumns(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const handleAddAsColumn = (key: string) => {
    setExtraColumns(prev => prev.includes(key) ? prev : [...prev, key]);
    setVisibleColumns(prev => new Set([...prev, key]));
  };

  const [timePreset, setTimePreset] = useState<string>('5m');
  const [autoRefresh, setAutoRefresh] = useState(() => new URLSearchParams(window.location.search).get('autoRefresh') === 'true');
  const [intervalPreset, setIntervalPreset] = useState<string>(() => new URLSearchParams(window.location.search).get('intervalPreset') ?? '30');
  const [customIntervalInput, setCustomIntervalInput] = useState<string>(() => new URLSearchParams(window.location.search).get('customInterval') ?? '');
  const [refreshIntervalSecs, setRefreshIntervalSecs] = useState<number>(() => {
    const p = new URLSearchParams(window.location.search);
    const ip = p.get('intervalPreset');
    if (ip && ip !== 'custom') return parseInt(ip);
    const ci = p.get('customInterval');
    return ci ? parseInt(ci) : 30;
  });

  const getPresetDates = (preset: string): { start: Date; end: Date } => {
    const end = new Date();
    const minutesMap: Record<string, number> = {
      '5m': 5, '15m': 15, '30m': 30, '1h': 60, '3h': 180, '24h': 1440,
    };
    return { start: new Date(end.getTime() - (minutesMap[preset] ?? 5) * 60 * 1000), end };
  };

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

  useEffect(() => {
    if ((location.state as { internal?: boolean })?.internal) return;
    const q = searchParams.get('query') ?? '';
    const start = searchParams.get('start');
    const end = searchParams.get('end');
    const traceOrSpanParam = searchParams.get('traceOrSpan');
    const sf = searchParams.get('sortField') as typeof sortField;
    const so = searchParams.get('sortOrder') as typeof sortOrder;
    const pg = parseInt(searchParams.get('page') || '1');
    const sz = parseInt(searchParams.get('pageSize') || '20');
    const svc = searchParams.get('service') ?? '';
    const presetParam = searchParams.get('timePreset') ?? '5m';
    const autoRefreshParam = searchParams.get('autoRefresh') === 'true';
    const intervalPresetParam = searchParams.get('intervalPreset') ?? '30';
    const customIntervalParam = searchParams.get('customInterval') ?? '';

    setQuery(q);
    setTimePreset(presetParam);
    setAutoRefresh(autoRefreshParam);
    setIntervalPreset(intervalPresetParam);
    setCustomIntervalInput(customIntervalParam);
    if (intervalPresetParam !== 'custom') {
      setRefreshIntervalSecs(parseInt(intervalPresetParam));
    } else if (customIntervalParam) {
      const secs = parseInt(customIntervalParam);
      if (!isNaN(secs) && secs >= 1) setRefreshIntervalSecs(secs);
    }
    if (start) setStartDate(new Date(start));
    if (end) setEndDate(new Date(end));
    if (sf) setSortField(sf);
    if (so) setSortOrder(so);
    if (!isNaN(pg)) setPage(pg);
    if (!isNaN(sz)) setPageSize(sz);
    if (svc) setSelectedService(svc);
    if (traceOrSpanParam) {
      setTraceOrSpan(traceOrSpanParam as "trace" | "span" | "all");
    }

    handleSearch(
      pg, q, sz,
      sf || sortField, so || sortOrder,
      start ? new Date(start) : startDate,
      end ? new Date(end) : endDate,
      svc,
      (traceOrSpanParam as "trace" | "span" | "all") || traceOrSpan,
      presetParam,
      false,
      true,
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.key]);

  const handleSearch = async (
    pageNum = 1,
    q = query,
    size = pageSize,
    sf = sortField,
    so = sortOrder,
    start = startDate,
    end = endDate,
    service = selectedService,
    traceOrSpanValue = traceOrSpan,
    preset = timePreset,
    silent = false,
    skipUrlPush = false,
  ) => {
    const resolvedStart = preset !== 'custom' ? getPresetDates(preset).start : start;
    const resolvedEnd   = preset !== 'custom' ? getPresetDates(preset).end   : end;

    if (!resolvedStart || !resolvedEnd || isNaN(resolvedStart.getTime()) || isNaN(resolvedEnd.getTime())) {
      setError('Invalid start or end date');
      return;
    }

    let effectiveQuery = q;
    const alreadyInQuery = q.split(',').some(p => p.trim().startsWith('service.name='));
    if (service && !alreadyInQuery) {
      const serviceFilter = `service.name=${service}`;
      effectiveQuery = q ? `${serviceFilter},${q}` : serviceFilter;
    }

    const params: Record<string, string> = {
      query: effectiveQuery,
      page: String(pageNum),
      pageSize: String(size),
      sortField: sf,
      sortOrder: so,
      start: resolvedStart.toISOString(),
      end: resolvedEnd.toISOString(),
      traceOrSpan: traceOrSpanValue,
    };

    const urlParams: Record<string, string> = {
      query: effectiveQuery,
      page: String(pageNum),
      pageSize: String(size),
      sortField: sf,
      sortOrder: so,
      traceOrSpan: traceOrSpanValue,
      timePreset: preset,
    };
    if (preset === 'custom') {
      urlParams.start = resolvedStart.toISOString();
      urlParams.end = resolvedEnd.toISOString();
    }
    if (service) urlParams.service = service;
    urlParams.columns = [...visibleColumns].join(',');
    if (extraColumns.length > 0) urlParams.extraColumns = extraColumns.join(',');
    urlParams.autoRefresh = String(autoRefresh);
    urlParams.intervalPreset = intervalPreset;
    if (intervalPreset === 'custom' && customIntervalInput) urlParams.customInterval = customIntervalInput;
    if (!silent && !skipUrlPush) { navigate(`?${new URLSearchParams(urlParams)}`, { replace: false, state: { internal: true } }); }
    if (!silent) { setLoading(true); setError(null); }

    try {
      const searchUrl = new URL(`${config.backendUrl}/v1/search`);
      Object.entries(params).forEach(([k, v]) => searchUrl.searchParams.set(k, v));

      const searchResponse = await fetch(searchUrl.toString());
      if (!searchResponse.ok) {
        const errText = await searchResponse.text();
        throw new Error(`Search failed: ${errText}`);
      }
      const searchData: SearchResponse = await searchResponse.json();
      setSearchResponse(searchData);
      setPage(pageNum);

      const metricsUrl = new URL(`${config.backendUrl}/api/metrics/search`);
      metricsUrl.searchParams.set('query', effectiveQuery);
      metricsUrl.searchParams.set('start', resolvedStart.toISOString());
      metricsUrl.searchParams.set('end', resolvedEnd.toISOString());
      metricsUrl.searchParams.set('traceOrSpan', traceOrSpanValue);

      const metricsResponse = await fetch(metricsUrl.toString());
      if (metricsResponse.ok) {
        const metricsData = await metricsResponse.json();
        setTraceCountSeries(metricsData.TraceCountResults || []);
      } else {
        console.error('Failed to fetch metrics:', await metricsResponse.text());
        setTraceCountSeries([]);
      }
    } catch (err) {
      if (!silent) {
        setError(err instanceof Error ? err.message : 'An error occurred');
        setSearchResponse(null);
        setTraceCountSeries([]);
      }
    } finally {
      if (!silent) setLoading(false);
    }
  };

  const handleSearchRef = useRef(handleSearch);
  useEffect(() => {
    handleSearchRef.current = handleSearch;
  });

  const silentRefreshRef = useRef(() => {});
  useEffect(() => {
    silentRefreshRef.current = () =>
      handleSearch(page, query, pageSize, sortField, sortOrder, startDate, endDate, selectedService, traceOrSpan, timePreset, true);
  });

  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(() => silentRefreshRef.current(), refreshIntervalSecs * 1000);
    return () => clearInterval(id);
  }, [autoRefresh, refreshIntervalSecs]);

  // Sync UI-only state changes (columns, auto-refresh) back to URL without triggering a search
  useEffect(() => {
    setSearchParams(prev => {
      const next = new URLSearchParams(prev.toString());
      next.set('columns', [...visibleColumns].join(','));
      if (extraColumns.length > 0) next.set('extraColumns', extraColumns.join(','));
      else next.delete('extraColumns');
      next.set('autoRefresh', String(autoRefresh));
      next.set('intervalPreset', intervalPreset);
      if (intervalPreset === 'custom' && customIntervalInput) next.set('customInterval', customIntervalInput);
      else next.delete('customInterval');
      return next;
    }, { replace: true } as never);
  }, [visibleColumns, extraColumns, autoRefresh, intervalPreset, customIntervalInput, setSearchParams]);


  const handlePageSizeChange = (e: SelectChangeEvent<number>) => {
    const newSize = e.target.value as number;
    setPageSize(newSize);
    handleSearch(1, query, newSize);
  };

  const handleSortChange = (field: 'start_time' | 'end_time' | 'duration') => {
    if (field === sortField) {
      const newOrder = sortOrder === 'asc' ? 'desc' : 'asc';
      setSortOrder(newOrder);
      handleSearch(1, query, pageSize, field, newOrder);
    } else {
      setSortField(field);
      setSortOrder('desc');
      handleSearch(1, query, pageSize, field, 'desc');
    }
  };

  const handleServiceChange = (e: SelectChangeEvent<string>) => {
    const newService = e.target.value;
    setSelectedService(newService);
    const parts = query.split(',').map(p => p.trim()).filter(p => p && !p.startsWith('service.name='));
    if (newService) parts.push(`service.name=${newService}`);
    const updatedQuery = parts.join(',');
    setQuery(updatedQuery);
    handleSearch(1, updatedQuery, pageSize, sortField, sortOrder, startDate, endDate, newService);
  };

  const handleTraceOrSpanChange = (e: SelectChangeEvent<string>) => {
    const newTraceOrSpan = e.target.value as "trace" | "span" | "all";
    setTraceOrSpan(newTraceOrSpan);
    handleSearch(1, query, pageSize, sortField, sortOrder, startDate, endDate, selectedService, newTraceOrSpan);
  };

  const handlePresetChange = (e: SelectChangeEvent<string>) => {
    const value = e.target.value;
    setTimePreset(value);
    if (value !== 'custom') {
      const { start, end } = getPresetDates(value);
      handleSearch(1, query, pageSize, sortField, sortOrder, start, end, selectedService, traceOrSpan, value);
    }
  };

  const handleIntervalPresetChange = (e: SelectChangeEvent<string>) => {
    const value = e.target.value;
    setIntervalPreset(value);
    if (value !== 'custom') {
      setRefreshIntervalSecs(parseInt(value));
    }
  };

  const commitCustomInterval = () => {
    const secs = parseInt(customIntervalInput);
    if (!isNaN(secs) && secs >= 1) {
      setRefreshIntervalSecs(secs);
    }
  };

  const formatTimestamp = (ns: number) => format(new Date(ns / 1e6), 'yyyy-MM-dd HH:mm:ss.SSS');
  const formatDuration = (ms: number) => `${ms.toFixed(2)} ms`;
  const hasResults = (searchResponse?.results?.length ?? 0) > 0;
  const hasMorePages = hasResults && searchResponse!.results!.length >= pageSize;

  return (
    <Box sx={{ p: 3, display: 'grid', gridTemplateColumns: 'repeat(12, 1fr)', gap: 2 }}>
      <Box sx={{ gridColumn: 'span 12', display: 'flex', flexDirection: 'column', gap: 1.5 }}>
      <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center' }}>

        <FormControl size="small" sx={{ minWidth: 150 }}>
          <InputLabel>Time Range</InputLabel>
          <Select value={timePreset} label="Time Range" onChange={handlePresetChange}>
            <MenuItem value="5m">Last 5 minutes</MenuItem>
            <MenuItem value="15m">Last 15 minutes</MenuItem>
            <MenuItem value="30m">Last 30 minutes</MenuItem>
            <MenuItem value="1h">Last 1 hour</MenuItem>
            <MenuItem value="3h">Last 3 hours</MenuItem>
            <MenuItem value="24h">Last 24 hours</MenuItem>
            <MenuItem value="custom">Custom</MenuItem>
          </Select>
        </FormControl>

        {timePreset === 'custom' && (
          <>
            <TextField
              label="Start Time"
              type="datetime-local"
              size="small"
              value={format(startDate, "yyyy-MM-dd'T'HH:mm:ss")}
              onChange={e => setStartDate(new Date(e.target.value))}
              InputLabelProps={{ shrink: true }}
              inputProps={{ step: 1 }}
            />
            <TextField
              label="End Time"
              type="datetime-local"
              size="small"
              value={format(endDate, "yyyy-MM-dd'T'HH:mm:ss")}
              onChange={e => setEndDate(new Date(e.target.value))}
              InputLabelProps={{ shrink: true }}
              inputProps={{ step: 1 }}
            />
          </>
        )}

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography variant="body2">Auto-refresh</Typography>
          <Switch
            checked={autoRefresh}
            onChange={e => setAutoRefresh(e.target.checked)}
            size="small"
          />
        </Box>

        {autoRefresh && (
          <>
            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel>Interval</InputLabel>
              <Select value={intervalPreset} label="Interval" onChange={handleIntervalPresetChange}>
                <MenuItem value="5">5s</MenuItem>
                <MenuItem value="10">10s</MenuItem>
                <MenuItem value="30">30s</MenuItem>
                <MenuItem value="60">1m</MenuItem>
                <MenuItem value="300">5m</MenuItem>
                <MenuItem value="custom">Custom</MenuItem>
              </Select>
            </FormControl>
            {intervalPreset === 'custom' && (
              <TextField
                label="Seconds"
                type="number"
                size="small"
                sx={{ width: 90 }}
                value={customIntervalInput}
                onChange={e => setCustomIntervalInput(e.target.value)}
                onBlur={commitCustomInterval}
                onKeyDown={e => { if (e.key === 'Enter') commitCustomInterval(); }}
                inputProps={{ min: 1 }}
              />
            )}
          </>
        )}

        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel shrink>Service</InputLabel>
          <Select value={selectedService} label="Service" onChange={handleServiceChange} displayEmpty renderValue={v => v || 'All Services'} notched>
            <MenuItem value="">All Services</MenuItem>
            {availableServices.map(service => (
              <MenuItem key={service} value={service}>{service}</MenuItem>
            ))}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel>Trace Or Span</InputLabel>
          <Select value={traceOrSpan} label="Trace Or Span" onChange={handleTraceOrSpanChange}>
            <MenuItem value="all">All</MenuItem>
            <MenuItem value="trace">Trace</MenuItem>
            <MenuItem value="span">Span</MenuItem>
          </Select>
        </FormControl>

      </Box>
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
        <FilterChipInput value={query} onChange={setQuery} onSearch={(q) => handleSearch(1, q)} />
        <Button variant="outlined" startIcon={<RefreshIcon />} onClick={() => handleSearch(1)} disabled={loading}>
          Refresh
        </Button>
      </Box>
      </Box>

      {error && (
        <Box sx={{ gridColumn: 'span 12' }}>
          <Typography color="error">{error}</Typography>
        </Box>
      )}
      {loading && (
        <Box sx={{ gridColumn: 'span 12', display: 'flex', justifyContent: 'center' }}>
          <CircularProgress />
        </Box>
      )}
      {!loading && searchResponse && (
        <Box sx={{ gridColumn: 'span 12' }}>
          <TraceCountChart
            data={traceCountSeries}
            onRangeSelect={(start, end) => {
              const s = new Date(start);
              const e = new Date(end);
              setStartDate(s);
              setEndDate(e);
              setTimePreset('custom');
              handleSearch(1, query, pageSize, sortField, sortOrder, s, e, selectedService, traceOrSpan, 'custom');
            }}
          />
        </Box>
      )}
      {!loading && (searchResponse?.results?.length ?? 0) > 0 && (
        <>
          <Box sx={{ gridColumn: 'span 12' }}>
            <Box sx={{ display: 'flex', justifyContent: 'flex-end', mb: 1 }}>
              <Button ref={columnsButtonRef} size="small" startIcon={<ViewColumnIcon />} onClick={() => setColumnAnchorEl(columnsButtonRef.current)}>
                Columns
              </Button>
              <Popover
                open={Boolean(columnAnchorEl)}
                anchorEl={columnAnchorEl}
                onClose={() => setColumnAnchorEl(null)}
                anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
                transformOrigin={{ vertical: 'top', horizontal: 'right' }}
              >
                <Box sx={{ p: 2, minWidth: 200 }}>
                  <Typography variant="caption" color="text.secondary">Default columns</Typography>
                  {[
                    { id: 'traceId', label: 'Trace ID' },
                    { id: 'spanId', label: 'Span ID' },
                    { id: 'name', label: 'Name' },
                    { id: 'service', label: 'Scope' },
                    { id: 'duration', label: 'Duration' },
                    { id: 'startTime', label: 'Start Time' },
                    { id: 'endTime', label: 'End Time' },
                  ].map(col => (
                    <FormControlLabel
                      key={col.id}
                      control={<Checkbox size="small" checked={visibleColumns.has(col.id)} onChange={() => toggleColumn(col.id)} />}
                      label={col.label}
                      sx={{ display: 'flex' }}
                    />
                  ))}
                  {extraColumns.length > 0 && (
                    <>
                      <Divider sx={{ my: 1 }} />
                      <Typography variant="caption" color="text.secondary">Attribute columns</Typography>
                      {extraColumns.map(key => (
                        <FormControlLabel
                          key={key}
                          control={<Checkbox size="small" checked={visibleColumns.has(key)} onChange={() => toggleColumn(key)} />}
                          label={key}
                          sx={{ display: 'flex' }}
                        />
                      ))}
                    </>
                  )}
                </Box>
              </Popover>
            </Box>
            <TableContainer component={Paper}>
              <Table>
                <TableHead>
                  <TableRow>
                    {visibleColumns.has('traceId') && <TableCell>Trace ID</TableCell>}
                    {visibleColumns.has('spanId') && <TableCell>Span ID</TableCell>}
                    {visibleColumns.has('name') && <TableCell>Name</TableCell>}
                    {visibleColumns.has('service') && <TableCell>Scope</TableCell>}
                    {visibleColumns.has('duration') && (
                      <TableCell onClick={() => handleSortChange('duration')} sx={{ cursor: 'pointer' }}>
                        Duration {sortField === 'duration' && (sortOrder === 'asc' ? '↑' : '↓')}
                      </TableCell>
                    )}
                    {visibleColumns.has('startTime') && (
                      <TableCell onClick={() => handleSortChange('start_time')} sx={{ cursor: 'pointer' }}>
                        Start Time {sortField === 'start_time' && (sortOrder === 'asc' ? '↑' : '↓')}
                      </TableCell>
                    )}
                    {visibleColumns.has('endTime') && (
                      <TableCell onClick={() => handleSortChange('end_time')} sx={{ cursor: 'pointer' }}>
                        End Time {sortField === 'end_time' && (sortOrder === 'asc' ? '↑' : '↓')}
                      </TableCell>
                    )}
                    {extraColumns.filter(k => visibleColumns.has(k)).map(key => (
                      <TableCell key={key}>{key}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {searchResponse?.results?.map((r, i) => (
                    <TableRow
                      key={`${r.TraceID}-${r.SpanID}-${i}`}
                      onClick={() => { setSelectedTraceId(r.TraceID); setSelectedSpanId(r.SpanID); }}
                      sx={{
                        cursor: 'pointer',
                        backgroundColor: r.hasError ? 'rgba(244, 67, 54, 0.1)' : 'inherit',
                        '&:hover': { backgroundColor: r.hasError ? 'rgba(244, 67, 54, 0.2)' : 'rgba(0,0,0,0.04)' }
                      }}
                    >
                      {visibleColumns.has('traceId') && <TableCell>{r.TraceID}</TableCell>}
                      {visibleColumns.has('spanId') && <TableCell>{r.SpanID}</TableCell>}
                      {visibleColumns.has('name') && <TableCell>{r.Name}</TableCell>}
                      {visibleColumns.has('service') && <TableCell>{r.Service}</TableCell>}
                      {visibleColumns.has('duration') && <TableCell>{formatDuration(r.Duration)}</TableCell>}
                      {visibleColumns.has('startTime') && <TableCell>{formatTimestamp(r.StartTime)}</TableCell>}
                      {visibleColumns.has('endTime') && <TableCell>{formatTimestamp(r.StartTime + r.Duration * 1e6)}</TableCell>}
                      {extraColumns.filter(k => visibleColumns.has(k)).map(key => (
                        <TableCell key={key}>{r.ResourceAttrs?.[key] ?? r.SpanAttrs?.[key] ?? '-'}</TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Box>
          <Box sx={{ gridColumn: 'span 12', display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 2 }}>
            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel>Page Size</InputLabel>
              <Select value={pageSize} label="Page Size" onChange={handlePageSizeChange}>
                {[10, 20, 50, 100].map(n => (
                  <MenuItem key={n} value={n}>{n}</MenuItem>
                ))}
              </Select>
            </FormControl>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Button disabled={page <= 1} onClick={() => handleSearch(page - 1)}>Previous</Button>
              <Typography>Page {page}</Typography>
              <Button disabled={!hasMorePages} onClick={() => handleSearch(page + 1)}>Next</Button>
            </Box>
          </Box>
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
            <Button
              size="small"
              startIcon={<OpenInNewIcon />}
              onClick={() => window.open(`/traces/${encodeURIComponent(selectedTraceId ?? '')}`, '_blank')}
            >
              Open in new tab
            </Button>
            <IconButton size="small" onClick={() => setSelectedTraceId(null)}>
              <CloseIcon />
            </IconButton>
          </Box>
        </Box>
        <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
          {selectedTraceId && (
            <TraceDetails
              traceId={selectedTraceId}
              initialSpanId={selectedSpanId ?? undefined}
              onAddToSearch={(key, value) => setQuery(prev => prev ? `${prev},${key}=${value}` : `${key}=${value}`)}
              onAddAsColumn={handleAddAsColumn}
            />
          )}
        </Box>
      </Drawer>
    </Box>
  );
};
