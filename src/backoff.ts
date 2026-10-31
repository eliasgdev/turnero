/**
 * "Full jitter" exponential backoff: a random delay between 0 and
 * min(max, base · 2^(attempt-1)). The randomness spreads out retries of jobs that
 * failed together, so a flaky dependency is not hit by all of them at once.
 */
export function retryDelayMs(attempt: number, baseMs: number, maxMs: number, random = Math.random): number {
  const ceiling = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.floor(random() * ceiling);
}
