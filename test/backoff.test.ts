import { describe, expect, it } from 'vitest';
import { retryDelayMs } from '../src/backoff.js';

describe('retryDelayMs', () => {
  it('doubles the ceiling with every attempt', () => {
    const max = () => 0.999999;
    expect(retryDelayMs(1, 1000, 60_000, max)).toBe(999);
    expect(retryDelayMs(2, 1000, 60_000, max)).toBe(1999);
    expect(retryDelayMs(3, 1000, 60_000, max)).toBe(3999);
  });

  it('never goes above the maximum delay', () => {
    expect(retryDelayMs(30, 1000, 60_000, () => 0.999999)).toBeLessThan(60_000);
  });

  it('can be zero, so retries spread over the whole window', () => {
    expect(retryDelayMs(5, 1000, 60_000, () => 0)).toBe(0);
  });
});
