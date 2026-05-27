import React, { useEffect, useState } from 'react';
import {
  Box, Typography, Table, TableBody, TableCell, TableContainer,
  TableHead, TableRow, Paper, Chip, CircularProgress, TableSortLabel,
} from '@mui/material';
import { config } from '../config';

interface LogRow {
  timestamp_unix_nano: number;
  severity_text: string;
  severity_number: number;
  body: string;
  scope_name: string;
  attributes: Record<string, string>;
}

const SEVERITY_COLORS: Record<string, 'default' | 'error' | 'warning' | 'info'> = {
  TRACE: 'default', DEBUG: 'default', INFO: 'info', WARN: 'warning', ERROR: 'error', FATAL: 'error',
};

const SEVERITY_LABELS: Record<number, string> = { 1: 'TRACE', 5: 'DEBUG', 9: 'INFO', 13: 'WARN', 17: 'ERROR', 21: 'FATAL' };

function severityLabel(text: string, num: number): string {
  if (text) return text.toUpperCase();
  const base = Math.ceil(num / 4) * 4 - 3;
  return SEVERITY_LABELS[base] ?? String(num);
}

type SortField = 'timestamp' | 'severity';
type SortDir = 'asc' | 'desc';

interface Props {
  spanId?: string;
  traceId?: string;
  startTimeNs?: number;
  endTimeNs?: number;
  title?: string;
}

const SpanLogsPanel: React.FC<Props> = ({ spanId, traceId, title = 'Logs' }) => {
  const [logs, setLogs] = useState<LogRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [sortField, setSortField] = useState<SortField>('timestamp');
  const [sortDir, setSortDir] = useState<SortDir>('asc');

  useEffect(() => {
    if (!spanId && !traceId) return;
    const params = new URLSearchParams({ pageSize: '500' });
    if (spanId) params.set('span_id', spanId);
    else if (traceId) params.set('trace_id', traceId);

    fetch(`${config.backendUrl}/api/logs?${params}`)
      .then(res => res.json())
      .then((data: { rows: LogRow[] } | LogRow[]) => setLogs(Array.isArray(data) ? data : (data.rows ?? [])))
      .catch(() => setLogs([]))
      .finally(() => setLoading(false));
  }, [spanId, traceId]);

  function handleSort(field: SortField) {
    if (sortField === field) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDir('asc');
    }
  }

  const sorted = [...logs].sort((a, b) => {
    let cmp = 0;
    if (sortField === 'timestamp') cmp = a.timestamp_unix_nano - b.timestamp_unix_nano;
    else cmp = a.severity_number - b.severity_number;
    return sortDir === 'asc' ? cmp : -cmp;
  });

  return (
    <Box mt={3}>
      <Typography variant="subtitle1" gutterBottom>{title}</Typography>
      {loading ? (
        <Box display="flex" justifyContent="center" py={2}><CircularProgress size={24} /></Box>
      ) : logs.length === 0 ? (
        <Typography variant="body2" color="text.secondary">No logs found.</Typography>
      ) : (
        <TableContainer component={Paper} variant="outlined">
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>
                  <TableSortLabel
                    active={sortField === 'timestamp'}
                    direction={sortField === 'timestamp' ? sortDir : 'asc'}
                    onClick={() => handleSort('timestamp')}
                  >
                    Timestamp
                  </TableSortLabel>
                </TableCell>
                <TableCell>
                  <TableSortLabel
                    active={sortField === 'severity'}
                    direction={sortField === 'severity' ? sortDir : 'asc'}
                    onClick={() => handleSort('severity')}
                  >
                    Severity
                  </TableSortLabel>
                </TableCell>
                <TableCell>Body</TableCell>
                <TableCell>Scope</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {sorted.map((log, idx) => {
                const sev = severityLabel(log.severity_text, log.severity_number);
                return (
                  <TableRow key={idx}>
                    <TableCell sx={{ whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: 11 }}>
                      {new Date(log.timestamp_unix_nano / 1e6).toLocaleString()}
                    </TableCell>
                    <TableCell>
                      <Chip label={sev} size="small" color={SEVERITY_COLORS[sev] ?? 'default'} />
                    </TableCell>
                    <TableCell sx={{ fontSize: 12, maxWidth: 500, wordBreak: 'break-word' }}>
                      {log.body}
                    </TableCell>
                    <TableCell sx={{ fontSize: 11, color: 'text.secondary' }}>{log.scope_name}</TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Box>
  );
};

export default SpanLogsPanel;
