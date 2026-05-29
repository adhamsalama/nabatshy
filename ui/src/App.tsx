import { useState, useEffect } from 'react';
import { Container, Box, Typography, AppBar, Toolbar, Button, IconButton, CssBaseline } from '@mui/material';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import LightModeIcon from '@mui/icons-material/LightMode';
import DarkModeIcon from '@mui/icons-material/DarkMode';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { TraceDetails } from './components/TraceDetails';
import { SpanDetails } from './components/SpanDetails';
import { TracesPage } from './components/TracesPage';
import { DashboardsPage } from './components/DashboardsPage';
import { CronPage } from './components/CronPage';
import { MetricsPage } from './components/MetricsPage';
import { LogsPage } from './components/LogsPage';
import { SQLPage } from './components/SQLPage';
import logo from '../../docs/assets/logo.png';
import { DemoModeContext } from './DemoModeContext';
import { config } from './config';

function App() {
  const [darkMode, setDarkMode] = useState(() => localStorage.getItem('darkMode') !== 'false');
  const [demoMode, setDemoMode] = useState(false);

  useEffect(() => {
    fetch(`${config.backendUrl}/api/config`)
      .then(r => r.json())
      .then(data => setDemoMode(data.demoMode ?? false))
      .catch(() => {});
  }, []);

  const theme = createTheme({
    palette: {
      mode: darkMode ? 'dark' : 'light',
      primary: darkMode
        ? { main: '#4db6ac', light: '#80cbc4', dark: '#2C6B6B', contrastText: '#000000' }
        : { main: '#2C6B6B', light: '#3d8f8f', dark: '#1e4d4d', contrastText: '#ffffff' },
      background: darkMode
        ? { default: '#121212', paper: '#1e1e1e' }
        : { default: '#f5f7f7', paper: '#ffffff' },
    },
    typography: {
      fontFamily: '"Inter", "system-ui", "-apple-system", sans-serif',
    },
    shape: {
      borderRadius: 8,
    },
    components: {
      MuiButton: {
        defaultProps: { disableElevation: true },
        styleOverrides: {
          root: { textTransform: 'none', fontWeight: 500 },
        },
      },
      MuiChip: {
        styleOverrides: {
          root: { fontWeight: 500 },
        },
      },
      MuiCard: {
        defaultProps: { elevation: 0 },
        styleOverrides: {
          root: ({ theme }) => ({
            border: `1px solid ${theme.palette.divider}`,
          }),
        },
      },
      MuiPaper: {
        defaultProps: { elevation: 0 },
        styleOverrides: {
          root: ({ theme }) => ({
            border: `1px solid ${theme.palette.divider}`,
          }),
        },
      },
      MuiPopover: {
        styleOverrides: {
          paper: ({ theme }) => ({
            boxShadow: theme.shadows[8],
          }),
        },
      },
      MuiTableHead: {
        styleOverrides: {
          root: ({ theme }) => ({
            backgroundColor: theme.palette.mode === 'dark'
              ? 'rgba(44, 107, 107, 0.15)'
              : 'rgba(44, 107, 107, 0.06)',
            '& .MuiTableCell-root': {
              fontWeight: 600,
              fontSize: '0.75rem',
              textTransform: 'uppercase',
              letterSpacing: '0.05em',
              color: theme.palette.text.secondary,
            },
          }),
        },
      },
    },
  });

  return (
    <DemoModeContext.Provider value={demoMode}>
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
                  style={{ height: 44, marginRight: 16 }}
                />
              </Typography>
            </Box>
            <Button disableRipple color="inherit" component={Link} to="/traces" sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              Traces
            </Button>
            <Button disableRipple color="inherit" component={Link} to="/metrics" sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              Metrics
            </Button>
            <Button disableRipple color="inherit" component={Link} to="/logs" sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              Logs
            </Button>
            <Button disableRipple color="inherit" component={Link} to="/dashboards" sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              Dashboards
            </Button>
            <Button disableRipple color="inherit" component={Link} to="/cron" sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              Cron
            </Button>
            <Button disableRipple color="inherit" component={Link} to="/sql" sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              SQL
            </Button>
            <IconButton disableRipple color="inherit" onClick={() => setDarkMode(d => { localStorage.setItem('darkMode', String(!d)); return !d; })} sx={{ color: 'white', '&:hover': { backgroundColor: 'rgba(255,255,255,0.15)', color: 'white' } }}>
              {darkMode ? <LightModeIcon /> : <DarkModeIcon />}
            </IconButton>
          </Toolbar>
        </AppBar>
        <Container maxWidth="lg" sx={{ py: 4 }}>
          <Routes>
            <Route path="/" element={<TracesPage />} />
            <Route path="/traces" element={<TracesPage />} />
            <Route path="/traces/:traceId" element={<TraceDetails />} />
            <Route path="/spans/:spanId" element={<SpanDetails />} />
            <Route path="/dashboards" element={<DashboardsPage />} />
            <Route path="/cron" element={<CronPage />} />
            <Route path="/metrics" element={<MetricsPage />} />
            <Route path="/logs" element={<LogsPage />} />
            <Route path="/sql" element={<SQLPage />} />
          </Routes>
        </Container>
      </Router>
    </ThemeProvider>
    </DemoModeContext.Provider>
  );
}

export default App;
