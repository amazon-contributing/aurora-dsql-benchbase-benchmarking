package com.oltpbenchmark.util;

import com.google.common.util.concurrent.RateLimiter;
import com.oltpbenchmark.api.BenchmarkModule;
import java.sql.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionUtil {
  private static final double DEFAULT_CONNECTION_RATE_LIMIT = 10.0;

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionUtil.class);

  private static RateLimiter rateLimiter = null;

  public static Connection makeConnectionWithRetry(BenchmarkModule benchmark) {
    int attempts = 0;
    while (attempts <= benchmark.getWorkloadConfiguration().getMaxRetries()) {
      try {
        getRateLimiter().acquire();
        Connection newConnection = benchmark.makeConnection();
        return newConnection;
      } catch (Exception e) {
        attempts++;
        if (attempts >= benchmark.getWorkloadConfiguration().getMaxRetries()) {
          throw new RuntimeException("Connection establishment attempts exhausted", e);
        }
        LOG.warn(
            "[Attempt: "
                + attempts
                + "]Connection establishment failed with exception, retrying...");
        // Wait before retrying
        try {
          // Exponential delay with jitter
          long delay = calExpDelay(attempts);
          Thread.sleep(delay);
        } catch (InterruptedException ie) {
          throw new RuntimeException("Interrupted while retrying connection establishment", ie);
        }
      }
    }
    return null;
  }

  private static long calExpDelay(int attempts) {
    long baseDelay = 1000; // in milliseconds
    double jitterFactor = 1.0;

    long delay = (long) (baseDelay * Math.pow(2, attempts));
    delay = (long) (delay * (1 + jitterFactor * Math.random()));
    delay = Math.min(delay, 4000);

    return delay;
  }

  private static synchronized RateLimiter getRateLimiter() {
    if (rateLimiter == null) {
      rateLimiter = RateLimiter.create(DEFAULT_CONNECTION_RATE_LIMIT);
    }
    return rateLimiter;
  }
}
