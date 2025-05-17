export interface Trace {
  TraceID: string;
  Name: string;
  Duration: number;
}

export interface TraceListResponse {
  traces: Trace[];
} 