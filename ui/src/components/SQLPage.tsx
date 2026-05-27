import React, { useState, useCallback, useEffect } from 'react';
import {
  Box, Button, Typography, Paper, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, CircularProgress, Alert,
  TextField, Accordion, AccordionSummary, AccordionDetails, Chip,
  ToggleButtonGroup, ToggleButton, Drawer, IconButton, Collapse,
} from '@mui/material';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import KeyboardArrowRightIcon from '@mui/icons-material/KeyboardArrowRight';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import CloseIcon from '@mui/icons-material/Close';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import { TraceDetails } from './TraceDetails';
import { config } from '../config';

interface ColumnInfo {
  table_name: string;
  column_name: string;
  data_type: string;
}

const DEFAULT_QUERY = `SELECT * REPLACE (
  resource_attributes::VARCHAR AS resource_attributes,
  span_attributes::VARCHAR AS span_attributes,
  events_attributes::VARCHAR AS events_attributes
) FROM denormalized_span LIMIT 10`;

export function SQLPage() {
  const [sql, setSql] = useState(DEFAULT_QUERY);
  const [rows, setRows] = useState<Record<string, unknown>[] | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set());
  const [schema, setSchema] = useState<Record<string, ColumnInfo[]>>({});
  const [schemaView, setSchemaView] = useState<'ddl' | 'columns'>('ddl');
  const [selectedTraceId, setSelectedTraceId] = useState<string | null>(null);
  const [selectedSpanId, setSelectedSpanId] = useState<string | null>(null);

  const handleIdClick = (col: string, value: string, row: Record<string, unknown>) => {
    if (col === 'trace_id') {
      setSelectedTraceId(value);
      setSelectedSpanId(null);
    } else {
      const traceId = String(row['trace_id'] ?? '');
      setSelectedTraceId(traceId || value);
      setSelectedSpanId(value);
    }
  };

  useEffect(() => {
    const fetchSchema = async () => {
      try {
        const q = `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' ORDER BY table_name, ordinal_position`;
        const res = await fetch(`${config.backendUrl}/debug/query?sql=${encodeURIComponent(q)}`);
        if (!res.ok) return;
        const data: ColumnInfo[] = await res.json();
        const grouped: Record<string, ColumnInfo[]> = {};
        for (const row of data) {
          if (!grouped[row.table_name]) grouped[row.table_name] = [];
          grouped[row.table_name].push(row);
        }
        setSchema(grouped);
      } catch {
        // schema panel is best-effort
      }
    };
    fetchSchema();
  }, []);

  const runQuery = useCallback(async () => {
    const query = sql.trim();
    if (!query) return;

    setLoading(true);
    setError(null);
    setRows(null);
    setColumns([]);

    try {
      const res = await fetch(
        `${config.backendUrl}/debug/query?sql=${encodeURIComponent(query)}`
      );
      const text = await res.text();
      if (!res.ok) {
        setError(text || `HTTP ${res.status}`);
        return;
      }
      const data: Record<string, unknown>[] = JSON.parse(text) ?? [];
      setRows(data);
      setColumns(data.length > 0 ? Object.keys(data[0]) : []);
      setExpandedRows(new Set());
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [sql]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      runQuery();
    }
  };

  const tableOrder = ['denormalized_span', 'metric_data_point', 'log_record', 'cron_jobs'];
  const tableNames = Object.keys(schema).sort((a, b) => {
    const ai = tableOrder.indexOf(a);
    const bi = tableOrder.indexOf(b);
    if (ai === -1 && bi === -1) return a.localeCompare(b);
    if (ai === -1) return 1;
    if (bi === -1) return -1;
    return ai - bi;
  });

  return (
    <Box>
      <Typography variant="h5" fontWeight={600} mb={3}>
        SQL Explorer
      </Typography>

      {tableNames.length > 0 && (
        <Accordion disableGutters sx={{ mb: 2 }}>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography variant="body2" fontWeight={600}>Database Schema</Typography>
          </AccordionSummary>
          <AccordionDetails sx={{ pt: 1 }}>
            <ToggleButtonGroup
              value={schemaView}
              exclusive
              onChange={(_, v) => { if (v) setSchemaView(v); }}
              size="small"
              sx={{ mb: 2 }}
            >
              <ToggleButton value="ddl">CREATE TABLE</ToggleButton>
              <ToggleButton value="columns">Columns</ToggleButton>
            </ToggleButtonGroup>

            {schemaView === 'ddl' ? (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                {tableNames.map(table => (
                  <Accordion key={table} disableGutters defaultExpanded={table === 'denormalized_span'} sx={{ '&:before': { display: 'none' }, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                    <AccordionSummary expandIcon={<ExpandMoreIcon />} sx={{ minHeight: 36, '& .MuiAccordionSummary-content': { my: 0.5 } }}>
                      <Typography variant="body2" sx={{ fontFamily: 'monospace', fontWeight: 600 }}>{table}</Typography>
                    </AccordionSummary>
                    <AccordionDetails sx={{ pt: 0 }}>
                      <Box
                        component="pre"
                        sx={{ m: 0, p: 1.5, bgcolor: 'action.hover', borderRadius: 1, fontFamily: 'monospace', fontSize: 12, overflowX: 'auto', whiteSpace: 'pre' }}
                      >
                        {`CREATE TABLE ${table} (\n${schema[table].map((col, i, arr) => `  ${col.column_name} ${col.data_type}${i < arr.length - 1 ? ',' : ''}${col.data_type.toUpperCase().includes('VARIANT') ? '  -- ⚠ not directly scannable' : ''}`).join('\n')}\n);`}
                      </Box>
                    </AccordionDetails>
                  </Accordion>
                ))}
              </Box>
            ) : (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                {tableNames.map(table => (
                  <Accordion key={table} disableGutters defaultExpanded={table === 'denormalized_span'} sx={{ '&:before': { display: 'none' }, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                    <AccordionSummary expandIcon={<ExpandMoreIcon />} sx={{ minHeight: 36, '& .MuiAccordionSummary-content': { my: 0.5 } }}>
                      <Typography variant="body2" sx={{ fontFamily: 'monospace', fontWeight: 600 }}>{table}</Typography>
                    </AccordionSummary>
                    <AccordionDetails sx={{ pt: 0 }}>
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75 }}>
                        {schema[table].map(col => {
                          const isVariant = col.data_type.toUpperCase().includes('VARIANT');
                          return (
                            <Chip
                              key={col.column_name}
                              label={`${col.column_name}: ${col.data_type}`}
                              size="small"
                              variant="outlined"
                              color={isVariant ? 'warning' : 'default'}
                              icon={isVariant ? <WarningAmberIcon /> : undefined}
                              sx={{ fontFamily: 'monospace', fontSize: 11 }}
                            />
                          );
                        })}
                      </Box>
                    </AccordionDetails>
                  </Accordion>
                ))}
              </Box>
            )}
          </AccordionDetails>
        </Accordion>
      )}

      <Paper sx={{ p: 2, mb: 2 }}>
        <TextField
          multiline
          fullWidth
          minRows={5}
          maxRows={20}
          value={sql}
          onChange={e => setSql(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={DEFAULT_QUERY}
          slotProps={{
            input: {
              sx: {
                fontFamily: 'monospace',
                fontSize: '0.875rem',
              },
            },
          }}
          variant="outlined"
        />
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mt: 1.5 }}>
          <Button
            variant="contained"
            startIcon={loading ? <CircularProgress size={16} color="inherit" /> : <PlayArrowIcon />}
            onClick={runQuery}
            disabled={loading}
          >
            Run
          </Button>
          <Typography variant="caption" color="text.secondary">
            Ctrl+Enter to run
          </Typography>
        </Box>
      </Paper>

      <Alert severity="info" sx={{ mb: 2 }}>
        <Typography variant="body2" fontWeight={600} gutterBottom>
          Querying MAP(VARCHAR, VARIANT) columns
        </Typography>
        <Typography variant="body2">
          Columns like <code>resource_attributes</code>, <code>span_attributes</code>, and <code>events_attributes</code> are stored as <code>MAP(VARCHAR, VARIANT)</code>.
          The Go driver cannot scan VARIANT values directly — use <code>SELECT * REPLACE (...::VARCHAR AS ...)</code> to cast them to strings, as shown in the default query.
        </Typography>
      </Alert>

      {error && (
        <Alert severity="error" sx={{ mb: 2, fontFamily: 'monospace', whiteSpace: 'pre-wrap' }}>
          {error}
        </Alert>
      )}

      {rows !== null && !error && (
        <>
          <Typography variant="body2" color="text.secondary" mb={1}>
            {rows.length} row{rows.length !== 1 ? 's' : ''} returned
          </Typography>
          {rows.length === 0 ? (
            <Alert severity="info">Query returned no rows.</Alert>
          ) : (
            <TableContainer component={Paper} sx={{ maxHeight: 600 }}>
              <Table stickyHeader size="small">
                <TableHead>
                  <TableRow>
                    <TableCell padding="none" sx={{ width: 32 }} />
                    {columns.map(col => (
                      <TableCell key={col}>{col}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {rows.map((row, i) => {
                    const expanded = expandedRows.has(i);
                    const toggle = () => setExpandedRows(prev => {
                      const next = new Set(prev);
                      next.has(i) ? next.delete(i) : next.add(i);
                      return next;
                    });
                    return (
                      <React.Fragment key={i}>
                        <TableRow hover onClick={toggle} sx={{ cursor: 'pointer' }}>
                          <TableCell padding="none" sx={{ width: 32, pl: 0.5 }}>
                            <IconButton size="small">
                              {expanded ? <KeyboardArrowDownIcon fontSize="small" /> : <KeyboardArrowRightIcon fontSize="small" />}
                            </IconButton>
                          </TableCell>
                          {columns.map(col => {
                            const isIdCol = col === 'span_id' || col === 'parent_span_id' || col === 'trace_id';
                            const val = row[col];
                            const strVal = val === null || val === undefined ? null : String(val);
                            return (
                              <TableCell
                                key={col}
                                sx={{
                                  maxWidth: 300,
                                  overflow: 'hidden',
                                  textOverflow: 'ellipsis',
                                  whiteSpace: 'nowrap',
                                  fontFamily: 'monospace',
                                  fontSize: '0.8125rem',
                                }}
                              >
                                {strVal === null
                                  ? <Typography component="span" variant="inherit" color="text.disabled">NULL</Typography>
                                  : isIdCol && strVal
                                    ? (
                                      <Box
                                        component="span"
                                        onClick={e => { e.stopPropagation(); handleIdClick(col, strVal, row); }}
                                        sx={{ cursor: 'pointer', textDecoration: 'underline dotted', textDecorationColor: 'primary.main', color: 'primary.main' }}
                                      >
                                        {strVal}
                                      </Box>
                                    )
                                    : strVal}
                              </TableCell>
                            );
                          })}
                        </TableRow>
                        {expanded && (
                          <TableRow>
                            <TableCell colSpan={columns.length + 1} sx={{ py: 0, bgcolor: 'action.hover' }}>
                              <Collapse in={expanded} unmountOnExit>
                                <Box sx={{ p: 1.5, display: 'flex', flexDirection: 'column', gap: 0.5 }}>
                                  {columns.map(col => {
                                    const val = row[col];
                                    const strVal = val === null || val === undefined ? null : String(val);
                                    return (
                                      <Box key={col} sx={{ display: 'flex', gap: 1, fontFamily: 'monospace', fontSize: 12 }}>
                                        <Typography component="span" sx={{ fontFamily: 'monospace', fontSize: 12, fontWeight: 700, flexShrink: 0, color: 'text.secondary' }}>
                                          {col}:
                                        </Typography>
                                        <Typography component="span" sx={{ fontFamily: 'monospace', fontSize: 12, wordBreak: 'break-all', whiteSpace: 'pre-wrap' }}>
                                          {strVal === null ? <span style={{ opacity: 0.4 }}>NULL</span> : strVal}
                                        </Typography>
                                      </Box>
                                    );
                                  })}
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
          )}
        </>
      )}
      <Drawer
        anchor="right"
        open={selectedTraceId !== null}
        onClose={() => { setSelectedTraceId(null); setSelectedSpanId(null); }}
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
            <IconButton size="small" onClick={() => { setSelectedTraceId(null); setSelectedSpanId(null); }}>
              <CloseIcon />
            </IconButton>
          </Box>
        </Box>
        <Box sx={{ flex: 1, overflow: 'auto', p: 2 }}>
          {selectedTraceId && (
            <TraceDetails
              traceId={selectedTraceId}
              initialSpanId={selectedSpanId ?? undefined}
              onAddToSearch={() => {}}
              onAddAsColumn={() => {}}
            />
          )}
        </Box>
      </Drawer>
    </Box>
  );
}
