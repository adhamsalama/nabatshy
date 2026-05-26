import React, { useState, useCallback } from 'react';
import {
  Box, Button, Typography, Paper, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, CircularProgress, Alert,
  TextField,
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import { config } from '../config';

export function SQLPage() {
  const [sql, setSql] = useState('SELECT * FROM denormalized_span LIMIT 10');
  const [rows, setRows] = useState<Record<string, unknown>[] | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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

  return (
    <Box>
      <Typography variant="h5" fontWeight={600} mb={3}>
        SQL Explorer
      </Typography>

      <Paper sx={{ p: 2, mb: 2 }}>
        <TextField
          multiline
          fullWidth
          minRows={5}
          maxRows={20}
          value={sql}
          onChange={e => setSql(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="SELECT * FROM denormalized_span LIMIT 10"
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
                    {columns.map(col => (
                      <TableCell key={col}>{col}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {rows.map((row, i) => (
                    <TableRow key={i} hover>
                      {columns.map(col => (
                        <TableCell
                          key={col}
                          sx={{
                            maxWidth: 400,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            fontFamily: 'monospace',
                            fontSize: '0.8125rem',
                          }}
                          title={String(row[col] ?? '')}
                        >
                          {row[col] === null || row[col] === undefined
                            ? <Typography component="span" variant="inherit" color="text.disabled">NULL</Typography>
                            : String(row[col])}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </>
      )}
    </Box>
  );
}
