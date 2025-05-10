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
import { useNavigate } from 'react-router-dom';
import SearchIcon from '@mui/icons-material/Search';
import InfoIcon from '@mui/icons-material/Info';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import { format } from 'date-fns';
import PercentileChart from './PercentileChart';
import TraceCountChart from './TraceCountChart';
import AvgDurationChart from './AvgDurationChart';

interface SearchResult {
  TraceID: string;
  SpanID: string;
  Name: string;
  Service: string;
  Duration: number;
  StartTime: number;
  ResourceAttrs: Record<string, string>;
}

interface TimePercentile {
  timestamp: string;
  value: number;
}

interface SearchResponse {
  results: SearchResult[];
  totalCount: number;
  page: number;
  pageSize: number;
  percentile: TimePercentile[];
  traceCount: TimePercentile[];
  avgDuration: TimePercentile[];
}

export const SearchPage: React.FC = () => {
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
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
  const [startDate, setStartDate] = useState(() => new Date(Date.now() - 5 * 60 * 1000));
  const [endDate, setEndDate] = useState(() => new Date());
  const navigate = useNavigate();

  const handleSearch = async (pageNum: number = 1) => {
    if (!startDate || !endDate || isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      setError('Invalid start or end date');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await fetch(
        `http://localhost:4318/v1/search?query=${encodeURIComponent(query)}&page=${pageNum}&pageSize=${pageSize}` +
        `&sortField=${sortField}&sortOrder=${sortOrder}` +
        `&start=${startDate.toISOString()}&end=${endDate.toISOString()}`
      );
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Search failed: ${errorText}`);
      }
      const data: SearchResponse = await response.json();
      setSearchResponse(data);
      setPercentileSeries(data.percentile);
      setTraceCountSeries(data.traceCount);
      setAvgDurationSeries(data.avgDuration);
      setPage(pageNum);
      setTotalCount(data.totalCount || 0);
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

  useEffect(() => {
    handleSearch(1);
  }, []);

  const handleKeyPress = (event: React.KeyboardEvent) => {
    if (event.key === 'Enter') {
      handleSearch(1);
    }
  };

  const handlePageChange = (_: React.ChangeEvent<unknown>, value: number) => {
    handleSearch(value);
  };

  const handlePageSizeChange = (event: SelectChangeEvent<number>) => {
    const newSize = event.target.value as number;
    setPageSize(newSize);
    handleSearch(1);
  };

  const handleSortChange = (field: 'start_time' | 'end_time' | 'duration') => {
    if (field === sortField) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortOrder('desc');
    }
    handleSearch(1);
  };

  const formatTimestamp = (ns: number) =>
    format(new Date(ns / 1e6), 'yyyy-MM-dd HH:mm:ss.SSS');

  const formatDuration = (ms: number) => `${ms.toFixed(2)} ms`;

  const totalPages = searchResponse
    ? Math.ceil(searchResponse.totalCount / searchResponse.pageSize)
    : 0;

  const toggleRow = (rowId: string) => {
    setExpandedRows(prev => {
      const next = new Set(prev);
      next.has(rowId) ? next.delete(rowId) : next.add(rowId);
      return next;
    });
  };

  return (
    <Box sx={{ p: 3, display: 'grid', gridTemplateColumns: 'repeat(12, 1fr)', gap: 2 }}>
      <Box sx={{ gridColumn: 'span 12' }}>
        <Typography variant="h4" gutterBottom>
          Search Traces
        </Typography>
      </Box>

      <Box sx={{ gridColumn: 'span 12', display: 'flex', gap: 2, flexWrap: 'wrap' }}>
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
      </Box>

      <Box sx={{ gridColumn: 'span 12', display: 'flex', gap: 2 }}>
        <TextField
          fullWidth
          placeholder="Search by trace, span, name, service, or attr..."
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

      {!loading && percentileSeries?.length > 0 && traceCountSeries?.length > 0 && avgDurationSeries?.length > 0 && (
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
            <PercentileChart data={percentileSeries} percentile={95} />
          </Box>
          <Box sx={{ flex: '1 1 30%', minWidth: 300 }}>
            <TraceCountChart data={traceCountSeries} />
          </Box>
          <Box sx={{ flex: '1 1 30%', minWidth: 300 }}>
            <AvgDurationChart data={avgDurationSeries} />
          </Box>
        </Box>
      )}

      {!loading && searchResponse && searchResponse.results?.length > 0 && (
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
                  {searchResponse.results.map((result, i) => {
                    const rowId = `${result.TraceID}-${result.SpanID}-${i}`;
                    const isExpanded = expandedRows.has(rowId);
                    const hasAttrs = Object.keys(result.ResourceAttrs).length > 0;
                    return (
                      <React.Fragment key={rowId}>
                        <TableRow
                          onClick={() => navigate(`/traces/${encodeURIComponent(result.TraceID)}`)}
                          sx={{ cursor: 'pointer', '&:hover': { backgroundColor: 'rgba(0,0,0,0.04)' } }}
                        >
                          <TableCell>{result.TraceID}</TableCell>
                          <TableCell>{result.SpanID}</TableCell>
                          <TableCell>{result.Name}</TableCell>
                          <TableCell>{result.Service}</TableCell>
                          <TableCell>{formatDuration(result.Duration)}</TableCell>
                          <TableCell>{formatTimestamp(result.StartTime)}</TableCell>
                          <TableCell>
                            {formatTimestamp(result.StartTime + result.Duration * 1e6)}
                          </TableCell>
                          <TableCell>
                            {hasAttrs && (
                              <Button
                                size="small"
                                onClick={e => {
                                  e.stopPropagation();
                                  toggleRow(rowId);
                                }}
                                endIcon={isExpanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                              >
                                {isExpanded ? 'Hide' : 'Show'}
                              </Button>
                            )}
                          </TableCell>
                        </TableRow>
                        {hasAttrs && (
                          <TableRow>
                            <TableCell colSpan={8} sx={{ p: 0 }}>
                              <Collapse in={isExpanded} timeout="auto" unmountOnExit>
                                <Box sx={{ p: 2, bgcolor: 'background.default' }}>
                                  <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                                    {Object.entries(result.ResourceAttrs).map(([k, v]) => (
                                      <Tooltip key={k} title={`${k}: ${v}`}>
                                        <Chip
                                          size="small"
                                          label={`${k}: ${v}`}
                                          icon={<InfoIcon />}
                                          onClick={e => e.stopPropagation()}
                                        />
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
                Showing {(page - 1) * pageSize + 1} to{' '}
                {Math.min(page * pageSize, totalCount)} of {totalCount} results
              </Typography>
              <Pagination count={totalPages} page={page} onChange={handlePageChange} />
            </Box>
          </Box>
        </>
      )}
    </Box>
  );
};

