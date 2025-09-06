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
  Chip,
  IconButton,
  Tooltip,
  Pagination,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  SelectChangeEvent,
  Collapse,
  Button,
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import InfoIcon from '@mui/icons-material/Info';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import RefreshIcon from '@mui/icons-material/Refresh';
import { format } from 'date-fns';
import { useSearchParams } from 'react-router-dom';
import PercentileChart, { TimePercentile } from './PercentileChart';
import TraceCountChart from './TraceCountChart';
import AvgDurationChart from './AvgDurationChart';
import { config } from "../config.ts";

interface SearchResult {
  TraceID: string;
  SpanID: string;
  Name: string;
  Service: string;
  Duration: number;
  StartTime: number;
  ResourceAttrs: Record<string, string>;
}

interface SearchResponse {
  results?: SearchResult[];
  totalCount: number;
  page: number;
  pageSize: number;
  percentile: TimePercentile[];
  traceCount: TimePercentile[];
  avgDuration: TimePercentile[];
}

const percentileOptions = [50, 75, 90, 95, 99, 100] as const;

export const SearchPage: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const [query, setQuery] = useState('');
  const [searchResponse, setSearchResponse] = useState<SearchResponse | null>(null);
  const [percentileSeries, setPercentileSeries] = useState<TimePercentile[]>([]);
  const [traceCountSeries, setTraceCountSeries] = useState<TimePercentile[]>([]);
  const [avgDurationSeries, setAvgDurationSeries] = useState<TimePercentile[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [totalCount, setTotalCount] = useState(0);
  const [sortField, setSortField] = useState<'start_time' | 'end_time' | 'duration'>('start_time');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [percentile, setPercentile] = useState<number>(95);
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
  const [startDate, setStartDate] = useState(() => new Date(Date.now() - 5 * 60 * 1000));
  const [endDate, setEndDate] = useState(() => new Date());

  useEffect(() => {
    const q = searchParams.get('query') ?? '';
    const start = searchParams.get('start');
    const end = searchParams.get('end');
    const sf = searchParams.get('sortField') as typeof sortField;
    const so = searchParams.get('sortOrder') as typeof sortOrder;
    const pg = parseInt(searchParams.get('page') || '1');
    const sz = parseInt(searchParams.get('pageSize') || '20');
    const perc = percentile ?? parseInt(searchParams.get('percentile') ?? percentile);

    setQuery(q);
    if (start) setStartDate(new Date(start));
    if (end) setEndDate(new Date(end));
    if (sf) setSortField(sf);
    if (so) setSortOrder(so);
    if (!isNaN(pg)) setPage(pg);
    if (!isNaN(sz)) setPageSize(sz);
    if (!isNaN(perc)) setPercentile(perc);

    handleSearch(pg, q, sz, sf, so, start ? new Date(start) : startDate, end ? new Date(end) : endDate, perc);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [percentile]);


  const handleSearch = async (
    pageNum = 1,
    q = query,
    size = pageSize,
    sf = sortField,
    so = sortOrder,
    start = startDate,
    end = endDate,
    perc = percentile
  ) => {
    const effectivePercentile = perc ?? percentile;

    if (!start || !end || isNaN(start.getTime()) || isNaN(end.getTime())) {
      setError('Invalid start or end date');
      return;
    }

    const params: Record<string, string> = {
      query: q,
      page: String(pageNum),
      pageSize: String(size),
      sortField: sf,
      sortOrder: so,
      start: start.toISOString(),
      end: end.toISOString(),
      percentile: String(effectivePercentile),
    };
    setSearchParams(params);
    setLoading(true);
    setError(null);

    try {
      const url = new URL(`${config.backendUrl}/v1/search`);
      Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));

      const response = await fetch(url.toString());
      if (!response.ok) {
        const errText = await response.text();
        throw new Error(`Search failed: ${errText}`);
      }
      const data: SearchResponse = await response.json();
      setSearchResponse(data);
      setPercentileSeries(data.percentile);
      setTraceCountSeries(data.traceCount);
      setAvgDurationSeries(data.avgDuration);
      setPage(pageNum);
      setTotalCount(data.totalCount);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setSearchResponse(null);
      setPercentileSeries([]);
      setTraceCountSeries([]);
      setAvgDurationSeries([]);
      setTotalCount(0);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') handleSearch(1);
  };

  const handlePageChange = (_: React.ChangeEvent<unknown>, v: number) => {
    setPage(v);
    handleSearch(v);
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

  const formatTimestamp = (ns: number) => format(new Date(ns / 1e6), 'yyyy-MM-dd HH:mm:ss.SSS');
  const formatDuration = (ms: number) => `${ms.toFixed(2)} ms`;
  const totalPages = searchResponse ? Math.ceil(searchResponse.totalCount / searchResponse.pageSize) : 0;
  const toggleRow = (id: string) =>
    setExpandedRows(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });

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
        <TextField
          fullWidth
          placeholder="service.name=auth,http.method!=GET"
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
        <Box
          sx={{
            gridColumn: 'span 12',
            display: 'flex',
            flexWrap: 'wrap',
            justifyContent: 'space-between',
            gap: 2,
          }}
        >
          <Box sx={{ flex: '1 1 30%', minWidth: 300 }}>
            <PercentileChart data={percentileSeries} percentile={percentile} />
          </Box>
          <Box sx={{ flex: '1 1 30%', minWidth: 300 }}>
            <TraceCountChart data={traceCountSeries} />
          </Box>
          <Box sx={{ flex: '1 1 30%', minWidth: 300 }}>
            <AvgDurationChart data={avgDurationSeries} />
          </Box>
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
                    <TableCell>Service</TableCell>
                    <TableCell onClick={() => handleSortChange('duration')} sx={{ cursor: 'pointer' }}>
                      Duration {sortField === 'duration' && (sortOrder === 'asc' ? '↑' : '↓')}
                    </TableCell>
                    <TableCell onClick={() => handleSortChange('start_time')} sx={{ cursor: 'pointer' }}>
                      Start Time {sortField === 'start_time' && (sortOrder === 'asc' ? '↑' : '↓')}
                    </TableCell>
                    <TableCell onClick={() => handleSortChange('end_time')} sx={{ cursor: 'pointer' }}>
                      End Time {sortField === 'end_time' && (sortOrder === 'asc' ? '↑' : '↓')}
                    </TableCell>
                    <TableCell>Attributes</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {searchResponse?.results?.map((r, i) => {
                    const rowId = `${r.TraceID}-${r.SpanID}-${i}`;
                    const isExp = expandedRows.has(rowId);
                    const hasA = Object.keys(r.ResourceAttrs).length > 0;
                    return (
                      <React.Fragment key={rowId}>
                        <TableRow
                          onClick={() => window.open(`/traces/${encodeURIComponent(r.TraceID)}`, '_blank')}
                          sx={{ cursor: 'pointer', '&:hover': { backgroundColor: 'rgba(0,0,0,0.04)' } }}
                        >
                          <TableCell>{r.TraceID}</TableCell>
                          <TableCell>{r.SpanID}</TableCell>
                          <TableCell>{r.Name}</TableCell>
                          <TableCell>{r.Service}</TableCell>
                          <TableCell>{formatDuration(r.Duration)}</TableCell>
                          <TableCell>{formatTimestamp(r.StartTime)}</TableCell>
                          <TableCell>{formatTimestamp(r.StartTime + r.Duration * 1e6)}</TableCell>
                          <TableCell>
                            {hasA && (
                              <Button
                                size="small"
                                onClick={e => {
                                  e.stopPropagation();
                                  toggleRow(rowId);
                                }}
                                endIcon={isExp ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                              >
                                {isExp ? 'Hide' : 'Show'}
                              </Button>
                            )}
                          </TableCell>
                        </TableRow>
                        {hasA && (
                          <TableRow>
                            <TableCell colSpan={8} sx={{ p: 0 }}>
                              <Collapse in={isExp} timeout="auto" unmountOnExit>
                                <Box sx={{ p: 2, bgcolor: 'background.default' }}>
                                  <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                                    {Object.entries(r.ResourceAttrs).map(([k, v]) => (
                                      <Tooltip key={k} title={`${k}: ${v}`}>
                                        <Chip size="small" label={`${k}: ${v}`} icon={<InfoIcon />} />
                                      </Tooltip>
                                    ))}
                                  </Box>
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
              <Typography>
                Showing {(page - 1) * pageSize + 1} to {Math.min(page * pageSize, totalCount)} of {totalCount} results
              </Typography>
              <Pagination count={totalPages} page={page} onChange={handlePageChange} />
            </Box>
          </Box>
        </>
      )}
    </Box>
  );
};

