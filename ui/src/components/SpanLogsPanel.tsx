import React, { useEffect, useState } from 'react';
import {
  Box, Typography, Table, TableBody, TableCell, TableContainer,
  TableHead, TableRow, Paper, Chip, CircularProgress,
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

interface Props {
  spanId: string;
  startTimeNs: number;
  endTimeNs: number;
}

const SpanLogsPanel: React.FC<Props> = ({ spanId, startTimeNs, endTimeNs }) => {
  const [logs, setLogs] = useState<LogRow[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!spanId) return;
    const params = new URLSearchParams({ span_id: spanId, pageSize: '100' });

    fetch(`${config.backendUrl}/api/logs?${params}`)
      .then(res => res.json())
      .then((data: LogRow[]) => setLogs(data ?? []))
      .catch(() => setLogs([]))
      .finally(() => setLoading(false));
  }, [spanId, startTimeNs, endTimeNs]);

  return (
    <Box mt={3}>
      <Typography variant="subtitle1" gutterBottom>Logs</Typography>
      {loading ? (
        <Box display="flex" justifyContent="center" py={2}><CircularProgress size={24} /></Box>
      ) : logs.length === 0 ? (
        <Typography variant="body2" color="text.secondary">No logs for this span.</Typography>
      ) : (
        <TableContainer component={Paper} variant="outlined">
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Timestamp</TableCell>
                <TableCell>Severity</TableCell>
                <TableCell>Body</TableCell>
                <TableCell>Scope</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {logs.map((log, idx) => {
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
