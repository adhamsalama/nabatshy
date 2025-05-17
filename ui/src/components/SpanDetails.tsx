import { Container, Typography, Paper, Box, Chip } from '@mui/material';

export interface SpanDetail {
  SpanID: string;
  TraceID: string;
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
  resourceAttributes?: Record<string, string>;
}

export const SpanDetails = ({ span }: { span?: SpanDetail | null }) => {
  if (!span) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="20vh">
        <Typography>Click a span row to view details here.</Typography>
      </Box>
    );
  }

  return (
    <Container>
      <Typography variant="h5" gutterBottom>
        Span Details: {span.Name}
      </Typography>
      <Paper sx={{ p: 3 }}>
        <Box display="flex" flexWrap="wrap" gap={4}>
          <Box flex={1} minWidth={250}>
            <Typography variant="subtitle1" gutterBottom>Basic Information</Typography>
            <Typography><strong>Span ID:</strong> {span.SpanID}</Typography>
            <Typography><strong>Trace ID:</strong> {span.TraceID}</Typography>
            <Typography><strong>Parent Span ID:</strong> {span.ParentSpanID || '-'}</Typography>
            <Typography><strong>Service:</strong> {span.Service}</Typography>
          </Box>
          <Box flex={1} minWidth={250}>
            <Typography variant="subtitle1" gutterBottom>Timing Information</Typography>
            <Typography><strong>Start Time:</strong> {new Date(span.StartTime / 1000000).toISOString()}</Typography>
            <Typography><strong>End Time:</strong> {new Date(span.EndTime / 1000000).toISOString()}</Typography>
            <Typography><strong>Duration:</strong> {span.Duration.toFixed(2)}ms</Typography>
          </Box>
        </Box>
        <Box mt={3}>
          <Typography variant="subtitle1" gutterBottom>Performance Metrics</Typography>
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
            {span.AvgDuration !== undefined && <Chip label={`Avg: ${span.AvgDuration.toFixed(2)}ms`} />}
            {span.P50Duration !== undefined && <Chip label={`P50: ${span.P50Duration.toFixed(2)}ms`} />}
            {span.P90Duration !== undefined && <Chip label={`P90: ${span.P90Duration.toFixed(2)}ms`} />}
            {span.P99Duration !== undefined && <Chip label={`P99: ${span.P99Duration.toFixed(2)}ms`} />}
            {span.DurationDiff !== undefined && (
              <Chip 
                label={`Diff: ${span.DurationDiff.toFixed(2)}%`}
                color={span.DurationDiff > 0 ? 'error' : 'success'}
              />
            )}
          </Box>
        </Box>
        {span.resourceAttributes && Object.keys(span.resourceAttributes).length > 0 && (
          <Box mt={3}>
            <Typography variant="subtitle1" gutterBottom>Resource Attributes</Typography>
            <Paper variant="outlined" sx={{ p: 2, background: '#fafafa' }}>
              <Box display="flex" flexWrap="wrap" gap={2}>
                {Object.entries(span.resourceAttributes).map(([key, value]) => (
                  <Box key={key} flexBasis="33%" minWidth={200} mb={1}>
                    <Typography variant="body2"><strong>{key}:</strong> {value}</Typography>
                  </Box>
                ))}
              </Box>
            </Paper>
          </Box>
        )}
      </Paper>
    </Container>
  );
}; 