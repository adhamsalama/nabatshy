import React, { useState, useEffect } from 'react';
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
    const sf = searchParams.get('sortField') as typeof sortField;
    const so = searchParams.get('sortOrder') as typeof sortOrder;
    const pg = parseInt(searchParams.get('page') || '1');
    const sz = parseInt(searchParams.get('pageSize') || '20');
    const svc = searchParams.get('service') ?? '';

    setQuery(q);
    if (start) setStartDate(new Date(start));
    if (end) setEndDate(new Date(end));
    if (sf) setSortField(sf);
    if (so) setSortOrder(so);
    if (!isNaN(pg)) setPage(pg);
    if (!isNaN(sz)) setPageSize(sz);
    if (svc) setSelectedService(svc);

    handleSearch(pg, q, sz, sf, so, start ? new Date(start) : startDate, end ? new Date(end) : endDate, svc);
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
    service = selectedService
  ) => {
    if (!start || !end || isNaN(start.getTime()) || isNaN(end.getTime())) {
      setError('Invalid start or end date');
      return;
    }

    // Automatically append service filter to query if a service is selected
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
      start: start.toISOString(),
      end: end.toISOString(),
    };
    if (service) {
      params.service = service;
    }
    setSearchParams(params);
    setLoading(true);
    setError(null);

    try {
      // Fetch search results
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

      // Fetch metrics separately
      const metricsUrl = new URL(`${config.backendUrl}/api/metrics/search`);
      metricsUrl.searchParams.set('query', effectiveQuery);
      metricsUrl.searchParams.set('start', start.toISOString());
      metricsUrl.searchParams.set('end', end.toISOString());

      const metricsResponse = await fetch(metricsUrl.toString());
      if (metricsResponse.ok) {
        const metricsData = await metricsResponse.json();
        setTraceCountSeries(metricsData.TraceCountResults || []);
      } else {
        // Don't fail the entire search if metrics fail
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

  const formatTimestamp = (ns: number) => format(new Date(ns / 1e6), 'yyyy-MM-dd HH:mm:ss.SSS');
  const formatDuration = (ms: number) => `${ms.toFixed(2)} ms`;
  // Show pagination with reasonable max (user can navigate as needed)
  const hasResults = (searchResponse?.results?.length ?? 0) > 0;
  const hasMorePages = hasResults && searchResponse!.results!.length >= pageSize;

  return (
    <Box sx={{ p: 3, display: 'grid', gridTemplateColumns: 'repeat(12, 1fr)', gap: 2 }}>
      <Box sx={{ gridColumn: 'span 12', display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center' }}>
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
        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel>Service</InputLabel>
          <Select
            value={selectedService}
            label="Service"
            onChange={handleServiceChange}
          >
            <MenuItem value="">All Services</MenuItem>
            {availableServices.map(service => (
              <MenuItem key={service} value={service}>{service}</MenuItem>
            ))}
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
        <Button
          variant="outlined"
          startIcon={<RefreshIcon />}
          onClick={() => handleSearch(1)}
          disabled={loading}
        >
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

          <Box
            sx={{
              gridColumn: 'span 12',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              mt: 2,
            }}
          >
            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel>Page Size</InputLabel>
              <Select value={pageSize} label="Page Size" onChange={handlePageSizeChange}>
                {[10, 20, 50, 100].map(n => (
                  <MenuItem key={n} value={n}>
                    {n}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Button
                disabled={page <= 1}
                onClick={() => handleSearch(page - 1)}
              >
                Previous
              </Button>
              <Typography>Page {page}</Typography>
              <Button
                disabled={!hasMorePages}
                onClick={() => handleSearch(page + 1)}
              >
                Next
              </Button>
            </Box>
          </Box>
        </>
      )}
    </Box>
  );
};

