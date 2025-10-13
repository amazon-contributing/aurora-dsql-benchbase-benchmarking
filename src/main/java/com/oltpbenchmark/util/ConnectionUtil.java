package com.oltpbenchmark.util;

import com.google.common.util.concurrent.RateLimiter;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.types.DatabaseType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** Connection utils with retries. */
public final class ConnectionUtil {
  private static final double DEFAULT_CONNECTION_RATE_LIMIT = 10.0;

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionUtil.class);

  private static RateLimiter rateLimiter = null;

  /** Connection operation string. */
  private static final String CONNECTION_OPERATION_SUCCESS = "CONNECTION-SUCCESS";

  /** Connection failed operation string. */
  private static final String CONNECTION_OPERATION_FAILED = "CONNECTION-FAILED";

  public static Connection makeConnectionWithRetry(BenchmarkModule benchmark) {
    final WorkloadConfiguration wrkld = benchmark.getWorkloadConfiguration();
    final DatabaseType dbType = wrkld.getDatabaseType();
    int attempts = 0;
    while (attempts <= benchmark.getWorkloadConfiguration().getMaxRetries()) {
      getRateLimiter().acquire();
      long startTimeNanos = System.nanoTime();
      try {
        Connection newConnection = benchmark.makeConnection();
        if (DatabaseType.AURORADSQL.equals(dbType)) {
          populateSessionIdOnThreadContext(newConnection);
        }
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
    delay = Math.min(delay, 4_000); // Cap the delay to 4s

    return delay;
  }

  private static synchronized RateLimiter getRateLimiter() {
    if (rateLimiter == null) {
      rateLimiter = RateLimiter.create(DEFAULT_CONNECTION_RATE_LIMIT);
    }
    return rateLimiter;
  }

  private static void populateSessionIdOnThreadContext(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      try (ResultSet rs = statement.executeQuery("SELECT sys.current_session_id()")) {
        if (rs.next()) {
          String sessionId = rs.getString(1);
          MDC.put("DBSessionId", sessionId);
          LOG.info(
              "Populated session id {} for thread {}", sessionId, Thread.currentThread().getName());
        }
      }
    }
  }
}
