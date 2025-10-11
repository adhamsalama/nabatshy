import { Container, Box, Typography, AppBar, Toolbar, Button } from '@mui/material';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { TraceDetails } from './components/TraceDetails';
import { SpanDetails } from './components/SpanDetails';
import { SearchPage } from './components/SearchPage';
import { MonitoringPage } from './components/MonitoringPage';
import logo from '../../docs/assets/logo.png'; // adjust path if needed

function App() {
  return (
    <Router>
      <AppBar position="static" sx={{ backgroundColor: '#2C6B6B' }}>
        <Toolbar>
          <Box sx={{ display: 'flex', alignItems: 'center', flexGrow: 1 }}>
            <Typography
              variant="h6"
              component={Link}
              to="/"
              sx={{
                textDecoration: 'none',
                color: 'inherit',
              }}
            >
              <img
                src={logo}
                alt="Logo"
                style={{ height: 50, marginRight: 16 }}
              />
            </Typography>
          </Box>
          <Button color="inherit" component={Link} to="/monitoring">
            Monitoring
          </Button>
          <Button color="inherit" component={Link} to="/search">
            Search
          </Button>
        </Toolbar>
      </AppBar>
      <Container maxWidth="lg" sx={{ py: 4 }}>
        <Routes>
          <Route path="/" element={<SearchPage />} />
          <Route path="/traces/:traceId" element={<TraceDetails />} />
          <Route path="/spans/:spanId" element={<SpanDetails />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/monitoring" element={<MonitoringPage />} />
        </Routes>
      </Container>
    </Router>
  );
}

export default App;
