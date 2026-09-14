package com.oltpbenchmark.util;

import com.google.common.util.concurrent.RateLimiter;

/** Utility for rate limiting connection creation. */
public final class ConnectionRateLimiterUtil {
  private static final double DEFAULT_CONNECTION_RATE_LIMIT = 10.0;
  private static RateLimiter rateLimiter;

  /**
   * Get a rate limiter for connection creation.
   *
   * @param rate The rate limit in connections per second
   * @return The rate limiter
   */
  public static synchronized RateLimiter getRateLimiter(double rate) {
    if (rateLimiter == null) {
      double effectiveRate = rate > 0 ? rate : DEFAULT_CONNECTION_RATE_LIMIT;
      rateLimiter = RateLimiter.create(effectiveRate);
    }
    return rateLimiter;
  }
}
