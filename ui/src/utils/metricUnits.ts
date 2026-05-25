// Canonical UCUM units for OpenTelemetry built-in semantic convention metrics.
// Used to fill in the correct unit when the SDK omits it (empty string in DB).
const OTEL_CANONICAL_UNITS: Record<string, string> = {
  // HTTP
  'http.client.duration': 'ms',
  'http.server.duration': 'ms',
  'http.client.request.duration': 'ms',
  'http.server.request.duration': 'ms',
  // Process CPU
  'process.cpu.time': 's',
  'process.cpu.utilization': '1',
  // Process memory / disk / network
  'process.memory.usage': 'By',
  'process.memory.virtual': 'By',
  'process.disk.io': 'By',
  'process.network.io': 'By',
  'process.open_file_descriptors': '{fd}',
  'process.threads': '{thread}',
  // System CPU
  'system.cpu.time': 's',
  'system.cpu.utilization': '1',
  'system.cpu.frequency': 'Hz',
  // System memory / swap
  'system.memory.usage': 'By',
  'system.memory.utilization': '1',
  'system.swap.usage': 'By',
  'system.swap.utilization': '1',
  // System disk / filesystem / paging
  'system.disk.io': 'By',
  'system.disk.io_time': 's',
  'system.disk.operation_time': 's',
  'system.disk.operations': '{operation}',
  'system.disk.merged': '{operation}',
  'system.filesystem.usage': 'By',
  'system.filesystem.utilization': '1',
  'system.paging.usage': 'By',
  'system.paging.utilization': '1',
  'system.paging.operations': '{operation}',
  'system.paging.faults': '{fault}',
  // System network
  'system.network.io': 'By',
  'system.network.packets': '{packet}',
  'system.network.dropped': '{packet}',
  'system.network.errors': '{error}',
  'system.network.connections': '{connection}',
  // System processes
  'system.processes.count': '{process}',
  'system.processes.created': '{process}',
};

/** Returns the DB unit if non-empty, else the canonical OTel unit, else empty string. */
export function resolveUnit(metricName: string, dbUnit: string): string {
  if (dbUnit) return dbUnit;
  return OTEL_CANONICAL_UNITS[metricName] ?? '';
}

function formatBytes(v: number): string {
  if (v < 1024) return `${v.toFixed(0)} B`;
  if (v < 1024 ** 2) return `${(v / 1024).toFixed(1)} KB`;
  if (v < 1024 ** 3) return `${(v / 1024 ** 2).toFixed(2)} MB`;
  return `${(v / 1024 ** 3).toFixed(2)} GB`;
}

/**
 * Format a metric value given its resolved unit.
 * Handles: bytes (By), time (s/ms), ratio (1→%), frequency (Hz), counts ({…}), generic SI.
 */
export function formatMetricValue(v: number, unit: string): string {
  if (unit === 's' || unit === 'ms') {
    const ms = unit === 'ms' ? v : v * 1000;
    if (ms < 1) return `${(ms * 1000).toFixed(2)} µs`;
    if (ms < 1000) return `${ms.toFixed(2)} ms`;
    const secs = ms / 1000;
    if (secs < 60) return `${secs.toFixed(2)} s`;
    const mins = secs / 60;
    if (mins < 60) return `${mins.toFixed(2)} min`;
    const hours = mins / 60;
    if (hours < 24) return `${hours.toFixed(2)} h`;
    return `${(hours / 24).toFixed(2)} d`;
  }
  if (unit === 'By') return formatBytes(v);
  if (unit === '1') return `${(v * 100).toFixed(2)}%`;
  if (unit === 'Hz') {
    if (v >= 1e9) return `${(v / 1e9).toFixed(2)} GHz`;
    if (v >= 1e6) return `${(v / 1e6).toFixed(2)} MHz`;
    if (v >= 1e3) return `${(v / 1e3).toFixed(2)} kHz`;
    return `${v.toFixed(0)} Hz`;
  }
  // Count units like {packet}, {error}, {fd}, etc. — show as integer
  if (unit.startsWith('{') && unit.endsWith('}')) {
    return v.toFixed(0);
  }
  // Generic SI fallback
  if (v === 0) return '0';
  if (Math.abs(v) < 0.0001) return v.toExponential(3);
  if (Math.abs(v) >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(2)}k`;
  return v.toFixed(4);
}

/** Short unit label to append after a formatted value (omitted for derived units). */
export function unitLabel(unit: string): string {
  if (!unit || unit === '1' || unit === 'By' || unit === 'Hz') return '';
  if (unit.startsWith('{')) return '';
  if (unit === 's' || unit === 'ms') return '';  // absorbed into formatMetricValue
  return unit;
}
