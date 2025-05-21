import { Container, Typography, AppBar, Toolbar, Button } from '@mui/material';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { TraceDetails } from './components/TraceDetails';
import { SpanDetails } from './components/SpanDetails';
import { SearchPage } from './components/SearchPage';

function App() {

  return (
    <Router>
      <AppBar position="static">
        <Toolbar>
          <Typography
            variant="h6"
            component={Link}
            to="/"
            sx={{
              flexGrow: 1,
              textDecoration: 'none',
              color: 'inherit',
            }}
          >
            Nabatshy
          </Typography>
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
        </Routes>
      </Container>
    </Router>
  );
}

export default App;
