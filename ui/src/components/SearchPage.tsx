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
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import RefreshIcon from '@mui/icons-material/Refresh';
import { format } from 'date-fns';
import { useSearchParams } from 'react-router-dom';
import TraceCountChart from './TraceCountChart';
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
}

interface SearchResponse {
  results?: SearchResult[];
  page: number;
  pageSize: number;
}

export const SearchPage: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [traceOrSpan, setTraceOrSpan] = useState<"trace" | "span">("trace");

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

  const [timePreset, setTimePreset] = useState<string>('5m');
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [intervalPreset, setIntervalPreset] = useState<string>('30');
  const [customIntervalInput, setCustomIntervalInput] = useState<string>('');
  const [refreshIntervalSecs, setRefreshIntervalSecs] = useState<number>(30);

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

    setQuery(q);
    setTimePreset(presetParam);
    if (start) setStartDate(new Date(start));
    if (end) setEndDate(new Date(end));
    if (sf) setSortField(sf);
    if (so) setSortOrder(so);
    if (!isNaN(pg)) setPage(pg);
    if (!isNaN(sz)) setPageSize(sz);
    if (svc) setSelectedService(svc);
    if (traceOrSpanParam) {
      setTraceOrSpan(traceOrSpanParam as "trace" | "span");
    }

    handleSearch(
      pg, q, sz,
      sf || sortField, so || sortOrder,
      start ? new Date(start) : startDate,
      end ? new Date(end) : endDate,
      svc,
      (traceOrSpanParam as "trace" | "span") || traceOrSpan,
      presetParam,
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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
  ) => {
    const resolvedStart = preset !== 'custom' ? getPresetDates(preset).start : start;
    const resolvedEnd   = preset !== 'custom' ? getPresetDates(preset).end   : end;

    if (!resolvedStart || !resolvedEnd || isNaN(resolvedStart.getTime()) || isNaN(resolvedEnd.getTime())) {
      setError('Invalid start or end date');
      return;
    }

    let effectiveQuery = q;
    if (service) {
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
    if (service) {
      urlParams.service = service;
    }
    setSearchParams(urlParams);
    setLoading(true);
    setError(null);

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
      setError(err instanceof Error ? err.message : 'An error occurred');
      setSearchResponse(null);
      setTraceCountSeries([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSearchRef = useRef(handleSearch);
  useEffect(() => {
    handleSearchRef.current = handleSearch;
  });

  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(() => handleSearchRef.current(1), refreshIntervalSecs * 1000);
    return () => clearInterval(id);
  }, [autoRefresh, refreshIntervalSecs]);

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') handleSearch(1);
  };

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
    handleSearch(1, query, pageSize, sortField, sortOrder, startDate, endDate, newService);
  };

  const handleTraceOrSpanChange = (e: SelectChangeEvent<string>) => {
    const newTraceOrSpan = e.target.value as "trace" | "span";
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
      <Box sx={{ gridColumn: 'span 12', display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center' }}>

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
          <InputLabel>Service</InputLabel>
          <Select value={selectedService} label="Service" onChange={handleServiceChange}>
            <MenuItem value="">All Services</MenuItem>
            {availableServices.map(service => (
              <MenuItem key={service} value={service}>{service}</MenuItem>
            ))}
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel>Trace Or Span</InputLabel>
          <Select value={traceOrSpan} label={traceOrSpan} onChange={handleTraceOrSpanChange}>
            <MenuItem key="Trace" value="trace">Trace</MenuItem>
            <MenuItem key="Span" value="span">Span</MenuItem>
          </Select>
        </FormControl>

        <TextField
          fullWidth
          placeholder="http.method!=GET,name=GetUser"
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyPress={handleKeyPress}
          InputProps={{
            endAdornment: (
              <IconButton onClick={() => handleSearch(1)} disabled={loading}>
                <SearchIcon />
              </IconButton>
            ),
          }}
        />

        <Button variant="outlined" startIcon={<RefreshIcon />} onClick={() => handleSearch(1)} disabled={loading}>
          Refresh
        </Button>
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
          <TraceCountChart data={traceCountSeries} />
        </Box>
      )}
      {!loading && (searchResponse?.results?.length ?? 0) > 0 && (
        <>
          <Box sx={{ gridColumn: 'span 12' }}>
            <TableContainer component={Paper}>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Trace ID</TableCell>
                    <TableCell>Span ID</TableCell>
                    <TableCell>Name</TableCell>
                    <TableCell>Scope</TableCell>
                    <TableCell onClick={() => handleSortChange('duration')} sx={{ cursor: 'pointer' }}>
                      Duration {sortField === 'duration' && (sortOrder === 'asc' ? '↑' : '↓')}
                    </TableCell>
                    <TableCell onClick={() => handleSortChange('start_time')} sx={{ cursor: 'pointer' }}>
                      Start Time {sortField === 'start_time' && (sortOrder === 'asc' ? '↑' : '↓')}
                    </TableCell>
                    <TableCell onClick={() => handleSortChange('end_time')} sx={{ cursor: 'pointer' }}>
                      End Time {sortField === 'end_time' && (sortOrder === 'asc' ? '↑' : '↓')}
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {searchResponse?.results?.map((r, i) => (
                    <TableRow
                      key={`${r.TraceID}-${r.SpanID}-${i}`}
                      onClick={() => window.open(`/traces/${encodeURIComponent(r.TraceID)}`, '_blank')}
                      sx={{
                        cursor: 'pointer',
                        backgroundColor: r.hasError ? 'rgba(244, 67, 54, 0.1)' : 'inherit',
                        '&:hover': { backgroundColor: r.hasError ? 'rgba(244, 67, 54, 0.2)' : 'rgba(0,0,0,0.04)' }
                      }}
                    >
                      <TableCell>{r.TraceID}</TableCell>
                      <TableCell>{r.SpanID}</TableCell>
                      <TableCell>{r.Name}</TableCell>
                      <TableCell>{r.Service}</TableCell>
                      <TableCell>{formatDuration(r.Duration)}</TableCell>
                      <TableCell>{formatTimestamp(r.StartTime)}</TableCell>
                      <TableCell>{formatTimestamp(r.StartTime + r.Duration * 1e6)}</TableCell>
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
    </Box>
  );
};
