import { useState } from 'react';
import { Container, Box, Typography, AppBar, Toolbar, Button, IconButton, CssBaseline } from '@mui/material';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import LightModeIcon from '@mui/icons-material/LightMode';
import DarkModeIcon from '@mui/icons-material/DarkMode';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { TraceDetails } from './components/TraceDetails';
import { SpanDetails } from './components/SpanDetails';
import { SearchPage } from './components/SearchPage';
import { MonitoringPage } from './components/MonitoringPage';
import { CronPage } from './components/CronPage';
import logo from '../../docs/assets/logo.png'; // adjust path if needed

function App() {
  const [darkMode, setDarkMode] = useState(() => localStorage.getItem('darkMode') !== 'false');

  const theme = createTheme({
    palette: {
      mode: darkMode ? 'dark' : 'light',
    },
  });

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
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
            <Button disableRipple color="inherit" component={Link} to="/monitoring" sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              Monitoring
            </Button>
            <Button disableRipple color="inherit" component={Link} to="/search" sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              Search
            </Button>
            <Button disableRipple color="inherit" component={Link} to="/cron" sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              Cron
            </Button>
            <IconButton disableRipple color="inherit" onClick={() => setDarkMode(d => { localStorage.setItem('darkMode', String(!d)); return !d; })} sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              {darkMode ? <LightModeIcon /> : <DarkModeIcon />}
            </IconButton>
          </Toolbar>
        </AppBar>
        <Container maxWidth="lg" sx={{ py: 4 }}>
          <Routes>
            <Route path="/" element={<SearchPage />} />
            <Route path="/traces/:traceId" element={<TraceDetails />} />
            <Route path="/spans/:spanId" element={<SpanDetails />} />
            <Route path="/search" element={<SearchPage />} />
            <Route path="/monitoring" element={<MonitoringPage />} />
            <Route path="/cron" element={<CronPage />} />
          </Routes>
        </Container>
      </Router>
    </ThemeProvider>
  );
}

export default App;
