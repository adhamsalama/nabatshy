import { useState } from 'react';

export interface ChartBrushHandlers {
  onMouseDown: (e: { activeLabel?: string } | null) => void;
  onMouseMove: (e: { activeLabel?: string } | null) => void;
  onMouseUp: () => void;
  refLeft: string | null;
  refRight: string | null;
  selecting: boolean;
}

export function useChartBrush(onRangeSelect?: (start: string, end: string) => void): ChartBrushHandlers {
  const [refLeft, setRefLeft] = useState<string | null>(null);
  const [refRight, setRefRight] = useState<string | null>(null);
  const [selecting, setSelecting] = useState(false);

  const onMouseDown = (e: { activeLabel?: string } | null) => {
    if (!onRangeSelect || !e?.activeLabel) return;
    setRefLeft(e.activeLabel);
    setRefRight(null);
    setSelecting(true);
  };

  const onMouseMove = (e: { activeLabel?: string } | null) => {
    if (!selecting || !e?.activeLabel) return;
    setRefRight(e.activeLabel);
  };

  const onMouseUp = () => {
    if (!selecting) return;
    setSelecting(false);
    if (refLeft && refRight && onRangeSelect) {
      const [start, end] = refLeft <= refRight ? [refLeft, refRight] : [refRight, refLeft];
      onRangeSelect(start, end);
    }
    setRefLeft(null);
    setRefRight(null);
  };

  return { onMouseDown, onMouseMove, onMouseUp, refLeft, refRight, selecting };
}
