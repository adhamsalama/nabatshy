import { useEffect, useState } from 'react';
import { useTheme } from '@mui/material/styles';
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
  StartTimeNS: number;
  EndTimeNS: number;
  DurationNS: number;
  AvgDuration?: number;
  P50Duration?: number;
  P90Duration?: number;
  P99Duration?: number;
  DurationDiff?: number;
  events: SpanEvent[];
}

export const TraceDetails = ({ traceId: traceIdProp, onAddToSearch, onAddAsColumn }: { traceId?: string; onAddToSearch?: (key: string, value: string) => void; onAddAsColumn?: (key: string) => void } = {}) => {
  const { traceId: traceIdParam } = useParams();
  const traceId = traceIdProp ?? traceIdParam;
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


  const traceMetadata = getSpansMetadata(spans);
  const totalDurationNS = traceMetadata.totalDuration;

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

  const SHOW_SPAN_TABLE = false;

  return (
    <Container>
      <Typography variant="h5" gutterBottom>
        Trace Details: {traceId}
      </Typography>
      {SHOW_SPAN_TABLE && <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell></TableCell>
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
                <TableCell>{new Date(span.StartTimeNS / 1000000).toISOString()}</TableCell>
                <TableCell>{new Date(span.EndTimeNS / 1000000).toISOString()}</TableCell>
                <TableCell>{(span.DurationNS / 1000000).toFixed(2)}ms</TableCell>
                <TableCell>{((span.DurationNS / totalDurationNS) * 100).toFixed(1)}%</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>}
      <Box mt={3}>
        <TraceDurationBars spans={spans} onSpanClick={setSelectedSpan} selectedSpanId={selectedSpan?.SpanID} />
      </Box>
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
          <SpanDetails span={spanDetail} onAddToSearch={onAddToSearch} onAddAsColumn={onAddAsColumn} />
        )}
      </Box>
    </Container>
  );
};

function getSpansMetadata(spans: TraceSpan[]) {
  const earliestSpan = spans.reduce((earliest, span) => span.StartTimeNS < earliest.StartTimeNS ? span : earliest, spans[0]);
  const latestSpan = spans.reduce((latest, span) => span.EndTimeNS > latest.EndTimeNS ? span : latest, spans[0]);
  const totalTraceDuration = latestSpan.EndTimeNS - earliestSpan.StartTimeNS;
  const totalTraceDurationMS = totalTraceDuration / 1000000;
  return {
    startTime: earliestSpan.StartTimeNS,
    endTime: latestSpan.EndTimeNS,
    totalDuration: totalTraceDuration,
    totalDurationMS: totalTraceDurationMS,
    earliestSpan,
    latestSpan
  }
}

const SERVICE_COLORS = [
  '#6366f1', '#0ea5e9', '#10b981', '#f59e0b', '#ec4899',
  '#8b5cf6', '#14b8a6', '#f97316', '#06b6d4', '#84cc16',
];



const TraceDurationBars = ({ spans, onSpanClick, selectedSpanId }: { spans: TraceSpan[], onSpanClick?: (span: TraceSpan) => void, selectedSpanId?: string }) => {
  const theme = useTheme();
  const isDark = theme.palette.mode === 'dark';
  const meta = getSpansMetadata(spans);
  const traceStart = meta.earliestSpan.StartTimeNS;
  const traceDurationNS = meta.endTime - meta.startTime;

  // Assign a consistent color per service name
  const services = [...new Set(spans.map(s => s.Service))];
  const serviceColor = (service: string) =>
    SERVICE_COLORS[services.indexOf(service) % SERVICE_COLORS.length];

  const hasError = (span: TraceSpan) =>
    span.events?.some(e => e.name === 'exception') ?? false;

  const fmtMs = (ns: number) => {
    const ms = ns / 1e6;
    return ms < 1 ? `${(ns / 1e3).toFixed(0)}µs` : `${ms.toFixed(2)}ms`;
  };

  // Time ruler tick marks
  const TICKS = 5;
  const ticks = Array.from({ length: TICKS + 1 }, (_, i) => i / TICKS);

  const NAME_COL = '38%';

  return (
    <Box sx={{ fontFamily: 'monospace', fontSize: 12 }}>
      {/* Time ruler */}
      <Box sx={{ display: 'flex', pl: NAME_COL, mb: 0.5 }}>
        <Box sx={{ position: 'relative', flex: 1, height: 20 }}>
          {ticks.map(t => (
            <Box key={t} sx={{ position: 'absolute', left: `${t * 100}%`, transform: 'translateX(-50%)', color: 'text.disabled', fontSize: 10, whiteSpace: 'nowrap' }}>
              {fmtMs(traceDurationNS * t)}
            </Box>
          ))}
        </Box>
      </Box>

      {/* Tick lines background */}
      <Box sx={{ display: 'flex', pl: NAME_COL, mb: 0.5 }}>
        <Box sx={{ position: 'relative', flex: 1, height: 1, background: isDark ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.08)' }}>
          {ticks.map(t => (
            <Box key={t} sx={{ position: 'absolute', left: `${t * 100}%`, top: -4, width: '1px', height: 8, background: isDark ? 'rgba(255,255,255,0.2)' : 'rgba(0,0,0,0.2)' }} />
          ))}
        </Box>
      </Box>

      {/* Span rows */}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
        {spans.map(span => {

          const offsetPct = (span.StartTimeNS - traceStart) / traceDurationNS * 100;
          const widthPct = Math.max(span.DurationNS / traceDurationNS * 100, 0.4);
          const isSelected = selectedSpanId === span.SpanID;
          const itemHasError = hasError(span);
          const pct = span.DurationNS / traceDurationNS * 100;
          const isRoot = !span.ParentSpanID;
          const color = itemHasError ? '#ef4444'
            : isRoot || pct < 25 ? serviceColor(span.Service)
            : pct >= 75 ? '#dc2626'
            : pct >= 50 ? '#ea580c'
            : '#eab308';
          const durationMs = fmtMs(span.DurationNS);

          return (
            <Tooltip
              key={span.SpanID}
              placement="top"
              title={
                <Box sx={{ fontSize: 12 }}>
                  <div><strong>{span.Name}</strong></div>
                  <div>{span.Service}</div>
                  <div>Duration: {durationMs}</div>
                  <div>Start: +{fmtMs(span.StartTimeNS - traceStart)}</div>
                  {itemHasError && <div style={{ color: '#fca5a5' }}>⚠ Exception</div>}
                </Box>
              }
            >
              <Box
                onClick={() => onSpanClick?.(span)}
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  height: 28,
                  cursor: 'pointer',
                  borderRadius: 1,
                  borderLeft: isSelected ? `3px solid ${color}` : '3px solid transparent',
                  background: isSelected
                    ? isDark ? 'rgba(99,102,241,0.15)' : 'rgba(99,102,241,0.08)'
                    : itemHasError ? 'rgba(239,68,68,0.08)' : 'transparent',
                  '&:hover': { background: itemHasError ? 'rgba(239,68,68,0.15)' : isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.04)' },
                }}
              >
                {/* Name column */}
                <Box sx={{ width: NAME_COL, flexShrink: 0, display: 'flex', alignItems: 'center', gap: 0.5, pl: 1, pr: 1, overflow: 'hidden' }}>
                  {itemHasError && <Box component="span" sx={{ color: '#ef4444', flexShrink: 0 }}>⚠</Box>}
                  <Box component="span" sx={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'text.primary', fontSize: 12, flex: 1 }}>
                    {span.Name}
                  </Box>
                  <Box component="span" sx={{ flexShrink: 0, color: 'text.disabled', fontSize: 11 }}>
                    {durationMs}
                  </Box>
                </Box>

                {/* Bar column */}
                <Box sx={{ flex: 1, position: 'relative', height: 16, background: isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)', borderRadius: 0.5, overflow: 'hidden' }}>
                  <Box sx={{
                    position: 'absolute',
                    left: `${offsetPct}%`,
                    width: `${widthPct}%`,
                    height: '100%',
                    background: color,
                    opacity: isSelected ? 1 : 0.8,
                    borderRadius: 0.5,
                  }} />
                </Box>
              </Box>
            </Tooltip>
          );
        })}
      </Box>
    </Box>
  );
}


