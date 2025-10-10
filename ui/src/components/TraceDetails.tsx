import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import {
  Container,
  Typography,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  CircularProgress,
  Box,
  Chip,
  Tooltip
} from '@mui/material';
import ErrorIcon from '@mui/icons-material/Error';
import { SpanDetails, SpanDetail } from './SpanDetails';
import { config } from "../config.ts";

interface SpanEvent {
  timeUnixNano: number;
  name: string;
  attributes?: Record<string, string>;
}

interface TraceSpan {
  SpanID: string;
  ParentSpanID: string;
  Name: string;
  Service: string;
  StartTime: number;
  EndTime: number;
  Duration: number;
  AvgDuration?: number;
  P50Duration?: number;
  P90Duration?: number;
  P99Duration?: number;
  DurationDiff?: number;
  events: SpanEvent[];
}

export const TraceDetails = () => {
  const { traceId } = useParams();
  const [spans, setSpans] = useState<TraceSpan[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSpan, setSelectedSpan] = useState<TraceSpan | null>(null);
  const [spanDetail, setSpanDetail] = useState<SpanDetail | null>(null);
  const [spanDetailLoading, setSpanDetailLoading] = useState(false);
  const [spanDetailError, setSpanDetailError] = useState<string | null>(null);

  useEffect(() => {
    const fetchTraceDetails = async () => {
      try {
        const response = await fetch(`${config.backendUrl}/v1/traces/${encodeURIComponent(traceId ?? '')}`);
        if (!response.ok) {
          throw new Error('Failed to fetch trace details');
        }
        const data = await response.json();
        setSpans(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load trace details');
      } finally {
        setLoading(false);
      }
    };
    fetchTraceDetails();
  }, [traceId]);

  // Fetch span details when selectedSpan changes
  useEffect(() => {
    if (!selectedSpan) {
      setSpanDetail(null);
      setSpanDetailError(null);
      return;
    }
    setSpanDetailLoading(true);
    setSpanDetailError(null);
    fetch(`${config.backendUrl}/v1/spans/${encodeURIComponent(selectedSpan.SpanID)}`)
      .then(res => {
        if (!res.ok) throw new Error('Failed to fetch span details');
        return res.json();
      })
      .then(data => setSpanDetail(data))
      .catch(err => setSpanDetailError(err instanceof Error ? err.message : 'Failed to load span details'))
      .finally(() => setSpanDetailLoading(false));
  }, [selectedSpan]);

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="50vh">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="50vh">
        <Typography color="error">{error}</Typography>
      </Box>
    );
  }

  const totalDuration = Math.max(...spans.map(s => s.Duration));

  const hasError = (span: TraceSpan) => {
    return span.events?.some(event => event.name === 'exception') || false;
  };

  const getExceptionDetails = (span: TraceSpan): string | null => {
    const exceptionEvent = span.events?.find(event => event.name === 'exception');
    if (!exceptionEvent?.attributes) return null;

    const type = exceptionEvent.attributes['exception.type'] || 'Error';
    const message = exceptionEvent.attributes['exception.message'] || 'Unknown error';
    const stacktrace = exceptionEvent.attributes['exception.stacktrace'];

    return `${type}: ${message}${stacktrace ? '\n\nStack trace:\n' + stacktrace : ''}`;
  };

  return (
    <Container>
      <Typography variant="h5" gutterBottom>
        Trace Details: {traceId}
      </Typography>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Status</TableCell>
              <TableCell>Span ID</TableCell>
              <TableCell>Parent Span ID</TableCell>
              <TableCell>Name</TableCell>
              <TableCell>Service</TableCell>
              <TableCell>Start Time</TableCell>
              <TableCell>End Time</TableCell>
              <TableCell>Duration</TableCell>
              <TableCell>% of Trace</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {spans.map((span) => (
              <TableRow
                key={span.SpanID}
                onClick={() => setSelectedSpan(span)}
                sx={{
                  cursor: 'pointer',
                  '&:hover': { backgroundColor: 'rgba(0, 0, 0, 0.04)' },
                  backgroundColor: hasError(span) ? 'rgba(239, 68, 68, 0.05)' : 'inherit'
                }}
              >
                <TableCell>
                  {hasError(span) && (
                    <Tooltip
                      title={
                        <Box sx={{ whiteSpace: 'pre-wrap', maxWidth: 400 }}>
                          {getExceptionDetails(span) || 'Exception occurred'}
                        </Box>
                      }
                      arrow
                    >
                      <Chip
                        icon={<ErrorIcon />}
                        label="Error"
                        color="error"
                        size="small"
                      />
                    </Tooltip>
                  )}
                </TableCell>
                <TableCell>{span.SpanID}</TableCell>
                <TableCell>{span.ParentSpanID || '-'}</TableCell>
                <TableCell>{span.Name}</TableCell>
                <TableCell>{span.Service}</TableCell>
                <TableCell>{new Date(span.StartTime / 1000000).toISOString()}</TableCell>
                <TableCell>{new Date(span.EndTime / 1000000).toISOString()}</TableCell>
                <TableCell>{span.Duration.toFixed(2)}ms</TableCell>
                <TableCell>{((span.Duration / totalDuration) * 100).toFixed(1)}%</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <TraceDurationBars spans={spans} onSpanClick={setSelectedSpan} selectedSpanId={selectedSpan?.SpanID} />
      <Box mt={4}>
        {spanDetailLoading ? (
          <Box display="flex" justifyContent="center" alignItems="center" minHeight="20vh">
            <CircularProgress />
          </Box>
        ) : spanDetailError ? (
          <Box display="flex" justifyContent="center" alignItems="center" minHeight="20vh">
            <Typography color="error">{spanDetailError}</Typography>
          </Box>
        ) : (
          <SpanDetails span={spanDetail} />
        )}
      </Box>
    </Container>
  );
};

const TraceDurationBars = ({ spans, onSpanClick, selectedSpanId }: { spans: TraceSpan[], onSpanClick?: (span: TraceSpan) => void, selectedSpanId?: string }) => {
  const rootSpan = spans[0];

  const hasError = (span: TraceSpan) => {
    return span.events?.some(event => event.name === 'exception') || false;
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {spans.map((item) => {
        const offsetPct = (item.StartTime - rootSpan.StartTime) / (rootSpan.EndTime - rootSpan.StartTime) * 100;
        const restOfLineDuration = rootSpan.EndTime - item.StartTime;
        const thisDuration = item.EndTime - item.StartTime;
        const widthPct = thisDuration / restOfLineDuration * 100;
        const percentage = item.Duration / rootSpan.Duration * 100;
        const itemHasError = hasError(item);

        let backgroundColor = '#4f46e5';
        if (itemHasError) {
          backgroundColor = '#dc2626'; // Red for errors
        } else if (percentage >= 75) {
          backgroundColor = '#dc2626'
        }
        else if (percentage >= 50) {
          backgroundColor = '#ea580c'
        }
        else if (percentage >= 25) {
          backgroundColor = '#eab308'
        }
        const isSelected = selectedSpanId === item.SpanID;
        return <div
          key={item.SpanID}
          style={{
            position: 'relative',
            width: '100%',
            height: 24,
            background: isSelected ? '#c7d2fe' : '#eee',
            borderRadius: 4,
            overflow: 'hidden',
            cursor: 'pointer',
            border: isSelected ? '2px solid #6366f1' : itemHasError ? '2px solid #dc2626' : '2px solid transparent',
          }}
          onClick={() => onSpanClick && onSpanClick(item)}
        >
          <div
            style={{
              position: 'absolute',
              left: `${offsetPct}%`,
              width: `${widthPct}%`,
              height: '100%',
              background: backgroundColor,
            }}
          />
          <div
            style={{
              position: 'absolute',
              top: 0,
              left: '50%',
              transform: 'translateX(-50%)',
              height: '100%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: 'white',
              fontSize: 12,
              whiteSpace: 'nowrap',
            }}
          >
            {itemHasError && '⚠️ '}
            {item.Name} ({item.Duration.toFixed(2)} ms, {percentage.toFixed(2)}%)
          </div>
        </div>
      })}
    </div>
  );
}


