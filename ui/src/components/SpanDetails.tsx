import { Container, Typography, Paper, Box, Chip, Alert, AlertTitle, IconButton, Tooltip } from '@mui/material';
import SpanLogsPanel from './SpanLogsPanel';
import ErrorIcon from '@mui/icons-material/Error';
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline';
import ViewColumnIcon from '@mui/icons-material/ViewColumn';

interface SpanEvent {
  timeUnixNano: number;
  name: string;
  attributes?: Record<string, string>;
}

export interface SpanDetail {
  SpanID: string;
  TraceID: string;
  ParentSpanID: string;
  Name: string;
  Kind?: string;
  Scope: string;
  StartTime: number;
  EndTime: number;
  Duration: number;
  AvgDuration?: number;
  P50Duration?: number;
  P90Duration?: number;
  P99Duration?: number;
  DurationDiff?: number;
  resourceAttributes?: Record<string, string>;
  spanAttributes?: Record<string, string>;
  events?: SpanEvent[];
}

const AttrRow = ({ attrKey, value, onAddToSearch, onAddAsColumn }: { attrKey: string; value: string; onAddToSearch?: (key: string, value: string) => void; onAddAsColumn?: (key: string) => void }) => (
  <Box
    display="flex"
    alignItems="flex-start"
    gap={1}
    sx={{ '&:hover .add-to-search': { visibility: 'visible' } }}
  >
    <Typography variant="body2" sx={{ flex: 1 }}>
      <strong>{attrKey}:</strong>{' '}
      {attrKey === 'db.statement' || attrKey === 'exception.stacktrace' ? (
        <Box component="pre" sx={{ mt: 1, p: 1, background: 'action.hover', border: '1px solid', borderColor: 'divider', borderRadius: '4px', fontSize: '0.75rem', overflow: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
          {value}
        </Box>
      ) : (
        <span>{value}</span>
      )}
    </Typography>
    {onAddToSearch && (
      <Tooltip title={`Add ${attrKey}=${value} to search`} placement="top">
        <IconButton
          className="add-to-search"
          size="small"
          sx={{ visibility: 'hidden', flexShrink: 0, mt: '-2px' }}
          onClick={() => onAddToSearch(attrKey, value)}
        >
          <AddCircleOutlineIcon fontSize="small" />
        </IconButton>
      </Tooltip>
    )}
    {onAddAsColumn && (
      <Tooltip title={`Add ${attrKey} as table column`} placement="top">
        <IconButton
          className="add-to-search"
          size="small"
          sx={{ visibility: 'hidden', flexShrink: 0, mt: '-2px' }}
          onClick={() => onAddAsColumn(attrKey)}
        >
          <ViewColumnIcon fontSize="small" />
        </IconButton>
      </Tooltip>
    )}
  </Box>
);

export const SpanDetails = ({ span, onAddToSearch, onAddAsColumn }: { span?: SpanDetail | null; onAddToSearch?: (key: string, value: string) => void; onAddAsColumn?: (key: string) => void }) => {
  if (!span) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="20vh">
        <Typography>Click a span row to view details here.</Typography>
      </Box>
    );
  }

  return (
    <Container>
      <Box display="flex" alignItems="center" gap={1} mb={1}>
        <Typography variant="h5">Span Details: {span.Name}</Typography>
        {onAddToSearch && (
          <Tooltip title={`Add name=${span.Name} to search`} placement="top">
            <IconButton size="small" onClick={() => onAddToSearch('name', span.Name)}>
              <AddCircleOutlineIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        )}
      </Box>
      <Paper sx={{ p: 3 }}>
        <Box display="flex" flexWrap="wrap" gap={4}>
          <Box flex={1} minWidth={250}>
            <Typography variant="subtitle1" gutterBottom>Basic Information</Typography>
            <Typography><strong>Span ID:</strong> {span.SpanID}</Typography>
            <Typography><strong>Trace ID:</strong> {span.TraceID}</Typography>
            <Typography><strong>Parent Span ID:</strong> {span.ParentSpanID || '-'}</Typography>
            <Typography><strong>Kind:</strong> {span.Kind || '-'}</Typography>
            <Typography><strong>Scope:</strong> {span.Scope}</Typography>
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
                label={`${Math.abs(span.DurationDiff).toFixed(2)}% ${span.DurationDiff > 0 ? 'slower' : 'faster'} than Avg`}
                color={span.DurationDiff > 0 ? 'error' : 'success'}
              />
            )}
          </Box>
        </Box>
        {span.spanAttributes && Object.keys(span.spanAttributes).length > 0 && (
          <Box mt={3}>
            <Typography variant="subtitle1" gutterBottom>Span Attributes</Typography>
            <Paper variant="outlined" sx={{ p: 2 }}>
              <Box display="flex" flexDirection="column" gap={1}>
                {Object.entries(span.spanAttributes).map(([key, value]) => (
                  <AttrRow key={key} attrKey={key} value={value} onAddToSearch={onAddToSearch} onAddAsColumn={onAddAsColumn} />
                ))}
              </Box>
            </Paper>
          </Box>
        )}
        {span.resourceAttributes && Object.keys(span.resourceAttributes).length > 0 && (
          <Box mt={3}>
            <Typography variant="subtitle1" gutterBottom>Resource Attributes</Typography>
            <Paper variant="outlined" sx={{ p: 2 }}>
              <Box display="flex" flexDirection="column" gap={1}>
                {Object.entries(span.resourceAttributes).map(([key, value]) => (
                  <AttrRow key={key} attrKey={key} value={value} onAddToSearch={onAddToSearch} onAddAsColumn={onAddAsColumn} />
                ))}
              </Box>
            </Paper>
          </Box>
        )}
        {span.events && span.events.length > 0 && (
          <Box mt={3}>
            <Typography variant="subtitle1" gutterBottom>Events</Typography>
            {span.events.map((event, idx) => {
              const isException = event.name === 'exception';
              return (
                <Alert
                  key={idx}
                  severity={isException ? 'error' : 'info'}
                  icon={isException ? <ErrorIcon /> : undefined}
                  sx={{ mb: 2 }}
                >
                  <AlertTitle>
                    {event.name} - {new Date(event.timeUnixNano / 1000000).toISOString()}
                  </AlertTitle>
                  {event.attributes && Object.keys(event.attributes).length > 0 && (
                    <Box mt={1}>
                      {Object.entries(event.attributes).map(([key, value]) => (
                        <Box key={key} mb={1}>
                          <Typography variant="body2">
                            <strong>{key}:</strong>
                            {key === 'exception.stacktrace' ? (
                              <Box component="pre" sx={{
                                mt: 1,
                                p: 1,
                                background: 'rgba(0, 0, 0, 0.05)',
                                border: '1px solid rgba(0, 0, 0, 0.1)',
                                borderRadius: '4px',
                                fontSize: '0.75rem',
                                overflow: 'auto',
                                whiteSpace: 'pre-wrap',
                                wordBreak: 'break-word'
                              }}>
                                {value}
                              </Box>
                            ) : (
                              <span> {value}</span>
                            )}
                          </Typography>
                        </Box>
                      ))}
                    </Box>
                  )}
                </Alert>
              );
            })}
          </Box>
        )}
      </Paper>
      <SpanLogsPanel spanId={span.SpanID} startTimeNs={span.StartTime} endTimeNs={span.EndTime} />
    </Container>
  );
};
