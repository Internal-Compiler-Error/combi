import { useEffect, useRef, useState, type CSSProperties } from "react";

const reducedMotion = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const easeOut = (t: number) => 1 - (1 - t) ** 3;

/**
 * Counts up to `value` from wherever the number last was (zero at first), so figures settle
 * instead of popping in. Jumps straight there for people who prefer reduced motion.
 */
export function useCountUp(value: number, ms = 700): number {
  const [shown, setShown] = useState(() => (reducedMotion() ? value : 0));
  const from = useRef(shown);
  useEffect(() => {
    if (reducedMotion() || from.current === value) {
      from.current = value;
      setShown(value);
      return;
    }
    const start = performance.now();
    const origin = from.current;
    let frame = requestAnimationFrame(function step(now) {
      const t = Math.min(1, (now - start) / ms);
      const v = Math.round(origin + (value - origin) * easeOut(t));
      from.current = v;
      setShown(v);
      if (t < 1) frame = requestAnimationFrame(step);
    });
    return () => cancelAnimationFrame(frame);
  }, [value, ms]);
  return shown;
}

const numberFormat = new Intl.NumberFormat();

export function CountUp({ value }: { value: number }) {
  return <>{numberFormat.format(useCountUp(value))}</>;
}

/** Position in a staggered entrance (`.rise` in styles.css); only the first dozen are delayed. */
export const rise = (i: number): CSSProperties => ({ "--i": Math.min(i, 12) }) as CSSProperties;
