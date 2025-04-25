import { useEffect, useState } from 'react';
import { Container, Typography, CircularProgress, Box, AppBar, Toolbar, Button } from '@mui/material';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { TraceTable } from './components/TraceTable';
import { TraceDetails } from './components/TraceDetails';
import { SpanDetails } from './components/SpanDetails';
import { fetchTraces } from './api/traces';
import { Trace } from './types/trace';
import { SearchPage } from './components/SearchPage';
import AnalyticsPage from './components/AnalyticsPage';

function App() {
  const [traces, setTraces] = useState<Trace[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadTraces = async () => {
      try {
        const response = await fetchTraces();
        setTraces(response);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load traces');
      } finally {
        setLoading(false);
      }
    };

    loadTraces();
  }, []);

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="100vh">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="100vh">
        <Typography color="error">{error}</Typography>
      </Box>
    );
  }

  return (
    <Router>
      <AppBar position="static">
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            Tracing Dashboard
          </Typography>
          <Button color="inherit" component={Link} to="/">
            Traces
          </Button>
          <Button color="inherit" component={Link} to="/search">
            Search
          </Button>
          <Button color="inherit" component={Link} to="/analytics">
            Analytics
          </Button>
        </Toolbar>
      </AppBar>
      <Container maxWidth="lg" sx={{ py: 4 }}>
        <Typography variant="h4" component="h1" gutterBottom>
          <Link to="/" style={{ textDecoration: 'none', color: 'inherit' }}>
            Trace Dashboard
          </Link>
        </Typography>
        <Routes>
          <Route path="/" element={<TraceTable traces={traces} />} />
          <Route path="/traces/:traceId" element={<TraceDetails />} />
          <Route path="/spans/:spanId" element={<SpanDetails />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/analytics" element={<AnalyticsPage />} />
        </Routes>
      </Container>
    </Router>
  );
}

export default App;
