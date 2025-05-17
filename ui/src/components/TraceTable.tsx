import { Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper } from '@mui/material';
import { useNavigate } from 'react-router-dom';
import { Trace } from '../types/trace';

interface TraceTableProps {
  traces: Trace[];
}

export const TraceTable = ({ traces }: TraceTableProps) => {
  const navigate = useNavigate();

  return (
    <TableContainer component={Paper}>
      <Table>
        <TableHead>
          <TableRow>
            <TableCell>Trace ID</TableCell>
            <TableCell>Name</TableCell>
            <TableCell>Duration</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {traces.map((trace) => (
            <TableRow 
              key={trace.TraceID}
              onClick={() => navigate(`/traces/${encodeURIComponent(trace.TraceID)}`)}
              sx={{ cursor: 'pointer', '&:hover': { backgroundColor: 'rgba(0, 0, 0, 0.04)' } }}
            >
              <TableCell>{trace.TraceID}</TableCell>
              <TableCell>{trace.Name}</TableCell>
              <TableCell>{trace.Duration.toFixed(2)}ms</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}; 