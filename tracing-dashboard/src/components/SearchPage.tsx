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
  Button
} from '@mui/material';
import { useNavigate } from 'react-router-dom';
import SearchIcon from '@mui/icons-material/Search';
import InfoIcon from '@mui/icons-material/Info';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import { format } from 'date-fns';

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
  results: SearchResult[];
  totalCount: number;
  page: number;
  pageSize: number;
}

export const SearchPage = () => {
  const [query, setQuery] = useState('');
  const [searchResponse, setSearchResponse] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [totalCount, setTotalCount] = useState(0);
  const [sortField, setSortField] = useState<'start_time' | 'end_time' | 'duration'>('start_time');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
  const navigate = useNavigate();

  const handleSearch = async (pageNum: number = 1) => {
    if (!query.trim()) return;

    setLoading(true);
    setError(null);

    try {
      const response = await fetch(
        `http://localhost:4318/v1/search?query=${encodeURIComponent(query)}&page=${pageNum}&pageSize=${pageSize}&sortField=${sortField}&sortOrder=${sortOrder}`
      );
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Search failed: ${errorText}`);
      }
      const data = await response.json();
      setSearchResponse(data);
      setPage(pageNum);
      setTotalCount(data.totalCount || 0);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setSearchResponse(null);
      setTotalCount(0);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (event: React.KeyboardEvent) => {
    if (event.key === 'Enter') {
      handleSearch(1);
    }
  };

  const handlePageChange = (event: React.ChangeEvent<unknown>, value: number) => {
    handleSearch(value);
  };

  const handlePageSizeChange = (event: SelectChangeEvent<number>) => {
    const newPageSize = event.target.value as number;
    setPageSize(newPageSize);
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

  const formatTimestamp = (timestamp: number) => {
    return format(new Date(timestamp / 1000000), 'yyyy-MM-dd HH:mm:ss.SSS');
  };

  const formatDuration = (duration: number) => {
    return `${duration.toFixed(2)}ms`;
  };

  const totalPages = searchResponse ? Math.ceil(searchResponse.totalCount / searchResponse.pageSize) : 0;

  const toggleRow = (rowId: string) => {
    setExpandedRows(prev => {
      const newSet = new Set(prev);
      if (newSet.has(rowId)) {
        newSet.delete(rowId);
      } else {
        newSet.add(rowId);
      }
      return newSet;
    });
  };

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" gutterBottom>
        Search Traces
      </Typography>

      <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
        <TextField
          fullWidth
          variant="outlined"
          placeholder="Search by trace ID, span ID, name, service, or resource attributes..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyPress={handleKeyPress}
          InputProps={{
            endAdornment: (
              <IconButton onClick={() => handleSearch(1)} disabled={loading}>
                <SearchIcon />
              </IconButton>
            ),
          }}
          helperText="You can search through trace IDs, span IDs, names, services, and resource attribute keys/values"
        />
      </Box>

      {error && (
        <Typography color="error" sx={{ mb: 2 }}>
          {error}
        </Typography>
      )}

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
          <CircularProgress />
        </Box>
      ) : !searchResponse ? (
        <Typography sx={{ mt: 2, textAlign: 'center' }}>
          Enter a search query to begin
        </Typography>
      ) : searchResponse.results.length === 0 ? (
        <Typography sx={{ mt: 2, textAlign: 'center' }}>
          No results found
        </Typography>
      ) : (
        <>
          <TableContainer component={Paper}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Trace ID</TableCell>
                  <TableCell>Span ID</TableCell>
                  <TableCell>Name</TableCell>
                  <TableCell>Service</TableCell>
                  <TableCell>
                    <Box 
                      sx={{ cursor: 'pointer', display: 'flex', alignItems: 'center' }}
                      onClick={() => handleSortChange('duration')}
                    >
                      Duration {sortField === 'duration' && (sortOrder === 'asc' ? '↑' : '↓')}
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Box 
                      sx={{ cursor: 'pointer', display: 'flex', alignItems: 'center' }}
                      onClick={() => handleSortChange('start_time')}
                    >
                      Start Time {sortField === 'start_time' && (sortOrder === 'asc' ? '↑' : '↓')}
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Box 
                      sx={{ cursor: 'pointer', display: 'flex', alignItems: 'center' }}
                      onClick={() => handleSortChange('end_time')}
                    >
                      End Time {sortField === 'end_time' && (sortOrder === 'asc' ? '↑' : '↓')}
                    </Box>
                  </TableCell>
                  <TableCell>Resource Attributes</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {searchResponse.results.map((result) => {
                  const rowId = `${result.TraceID}-${result.SpanID}`;
                  const isExpanded = expandedRows.has(rowId);
                  const hasAttributes = Object.keys(result.ResourceAttrs || {}).length > 0;

                  return (
                    <React.Fragment key={rowId}>
                      <TableRow
                        onClick={() => navigate(`/traces/${encodeURIComponent(result.TraceID)}`)}
                        sx={{ cursor: 'pointer', '&:hover': { backgroundColor: 'rgba(0, 0, 0, 0.04)' } }}
                      >
                        <TableCell>{result.TraceID}</TableCell>
                        <TableCell>{result.SpanID}</TableCell>
                        <TableCell>{result.Name}</TableCell>
                        <TableCell>{result.Service}</TableCell>
                        <TableCell>{formatDuration(result.Duration)}</TableCell>
                        <TableCell>{formatTimestamp(result.StartTime)}</TableCell>
                        <TableCell>{formatTimestamp(result.StartTime + result.Duration)}</TableCell>
                        <TableCell>
                          {hasAttributes && (
                            <Button
                              size="small"
                              onClick={(e) => {
                                e.stopPropagation();
                                toggleRow(rowId);
                              }}
                              endIcon={isExpanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                            >
                              {isExpanded ? 'Hide' : 'Show'} Attributes
                            </Button>
                          )}
                        </TableCell>
                      </TableRow>
                      {hasAttributes && (
                        <TableRow>
                          <TableCell colSpan={8} sx={{ p: 0 }}>
                            <Collapse in={isExpanded} timeout="auto" unmountOnExit>
                              <Box sx={{ p: 2, bgcolor: 'background.default' }}>
                                <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                                  {Object.entries(result.ResourceAttrs || {}).map(([key, value]) => (
                                    <Tooltip key={key} title={`${key}: ${value}`}>
                                      <Chip
                                        size="small"
                                        label={`${key}: ${value}`}
                                        icon={<InfoIcon />}
                                        onClick={(e) => {
                                          e.stopPropagation();
                                        }}
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

          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 2 }}>
            <FormControl size="small" sx={{ minWidth: 120 }}>
              <InputLabel>Page Size</InputLabel>
              <Select
                value={pageSize}
                label="Page Size"
                onChange={handlePageSizeChange}
              >
                <MenuItem value={10}>10</MenuItem>
                <MenuItem value={20}>20</MenuItem>
                <MenuItem value={50}>50</MenuItem>
                <MenuItem value={100}>100</MenuItem>
              </Select>
            </FormControl>

            <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Typography>
                Showing {((page - 1) * pageSize) + 1} to {Math.min(page * pageSize, totalCount)} of {totalCount} results
              </Typography>
              <Pagination
                count={totalPages}
                page={page}
                onChange={handlePageChange}
                color="primary"
              />
            </Box>
          </Box>
        </>
      )}
    </Box>
  );
}; 
