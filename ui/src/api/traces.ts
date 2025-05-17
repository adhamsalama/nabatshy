import { Trace } from '../types/trace';

const API_BASE_URL = 'http://localhost:4318';

export const fetchTraces = async (): Promise<Trace[]> => {
  const response = await fetch(`${API_BASE_URL}/v1/traces/slowest?n=100`);
  if (!response.ok) {
    throw new Error('Failed to fetch traces');
  }
  return response.json();
}; 