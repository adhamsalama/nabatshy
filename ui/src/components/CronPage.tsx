import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Button,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
  CircularProgress,
  Alert,
} from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import { config } from '../config.ts';
import { useDemoMode } from '../DemoModeContext';

interface CronJob {
  id: string;
  name: string;
  query: string;
  interval_seconds: number;
  created_at: number;
}

export const CronPage: React.FC = () => {
  const demoMode = useDemoMode();
  const [jobs, setJobs] = useState<CronJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState('');
  const [query, setQuery] = useState('');
  const [intervalSeconds, setIntervalSeconds] = useState('60');
  const [submitting, setSubmitting] = useState(false);

  const fetchJobs = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${config.backendUrl}/api/crons`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setJobs(data ?? []);
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchJobs();
  }, [fetchJobs]);

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const res = await fetch(`${config.backendUrl}/api/crons`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          query,
          interval_seconds: parseInt(intervalSeconds, 10),
        }),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      setName('');
      setQuery('');
      setIntervalSeconds('60');
      await fetchJobs();
    } catch (err) {
      setError(String(err));
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (id: string) => {
    setError(null);
    try {
      const res = await fetch(`${config.backendUrl}/api/crons/${id}`, {
        method: 'DELETE',
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      await fetchJobs();
    } catch (err) {
      setError(String(err));
    }
  };

  const formatCreatedAt = (nanos: number) => {
    return new Date(nanos / 1_000_000).toLocaleString();
  };

  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Cron Jobs
      </Typography>

      <Alert severity="info" sx={{ mb: 3 }}>
        Cron jobs run SQL queries on a schedule against your local DuckDB store. Use them to implement <strong>data retention policies</strong> — for example, deleting spans older than a certain age to keep storage in check.
      </Alert>

      {demoMode && (
        <Alert severity="info" sx={{ mb: 3 }}>
          Cron jobs are not available in demo mode.
        </Alert>
      )}

      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          New Cron Job
        </Typography>
        <Box component="form" onSubmit={handleCreate} sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <TextField
            label="Name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
            size="small"
            placeholder="Delete spans older than 1 day"
            InputLabelProps={{ shrink: true }}
            disabled={demoMode}
          />
          <TextField
            label="Query"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            required
            multiline
            rows={3}
            size="small"
            placeholder="DELETE FROM denormalized_span WHERE start_time_unix_nano < epoch_ns(now() - INTERVAL 1 DAY)"
            InputLabelProps={{ shrink: true }}
            disabled={demoMode}
          />
          <TextField
            label="Interval (seconds)"
            type="number"
            value={intervalSeconds}
            onChange={(e) => setIntervalSeconds(e.target.value)}
            required
            size="small"
            inputProps={{ min: 1 }}
            sx={{ maxWidth: 200 }}
            disabled={demoMode}
          />
          <Box>
            <Button
              type="submit"
              variant="contained"
              disabled={submitting || demoMode}
              sx={{ backgroundColor: '#2C6B6B', '&:hover': { backgroundColor: '#235555' } }}
            >
              {submitting ? <CircularProgress size={20} /> : 'Create'}
            </Button>
          </Box>
        </Box>
      </Paper>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
          <CircularProgress />
        </Box>
      ) : (
        <TableContainer component={Paper}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Query</TableCell>
                <TableCell>Interval (s)</TableCell>
                <TableCell>Created At</TableCell>
                <TableCell align="center">Delete</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {jobs.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} align="center">
                    No cron jobs yet.
                  </TableCell>
                </TableRow>
              ) : (
                jobs.map((job) => (
                  <TableRow key={job.id}>
                    <TableCell>{job.name}</TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', whiteSpace: 'pre-wrap', maxWidth: 400 }}>
                      {job.query}
                    </TableCell>
                    <TableCell>{job.interval_seconds}</TableCell>
                    <TableCell>{formatCreatedAt(job.created_at)}</TableCell>
                    <TableCell align="center">
                      <IconButton
                        size="small"
                        color="error"
                        onClick={() => handleDelete(job.id)}
                        aria-label="delete"
                        disabled={demoMode}
                      >
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Box>
  );
};
