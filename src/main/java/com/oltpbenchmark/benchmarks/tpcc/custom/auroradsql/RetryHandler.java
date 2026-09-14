/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql;

import com.oltpbenchmark.util.TimeUtil;
import java.sql.SQLException;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles retry logic for database operations with exponential backoff. Provides a clean
 * abstraction for retrying operations that may fail due to transient errors.
 */
@Slf4j
@RequiredArgsConstructor
public class RetryHandler {

  private final int maxRetries;
  private final ConnectionManager connectionManager;

  /**
   * Executes an operation with retry logic.
   *
   * @param operation The operation to execute
   * @param threadName The thread name for connection management
   * @param operationName Name of the operation for logging
   * @param <T> Return type of the operation
   * @return The result of the operation
   * @throws RuntimeException if all retry attempts are exhausted
   */
  public <T> T executeWithRetry(Supplier<T> operation, String threadName, String operationName) {
    int attempts = 0;

    while (attempts <= maxRetries) {
      try {
        return operation.get();
      } catch (Exception e) {
        attempts++;

        if (isDuplicateKeyException(e)) {
          log.warn("Skipping {} due to duplicate key violation", operationName);
          return null;
        }

        if (attempts > maxRetries) {
          throw new RuntimeException(
              String.format("All retry attempts exhausted for %s", operationName), e);
        }

        log.error(
            "Operation {} failed (attempt {}/{}), retrying...",
            operationName,
            attempts,
            maxRetries,
            e);

        handleRetryDelay(attempts);
        handleConnectionRecovery(threadName, e);
      }
    }

    throw new RuntimeException(
        String.format("Failed to execute %s after %d attempts", operationName, maxRetries));
  }

  /**
   * Executes a database operation with retry logic specifically for SQL operations.
   *
   * @param operation The SQL operation to execute
   * @param threadName The thread name for connection management
   * @param operationName Name of the operation for logging
   * @throws SQLException if the operation fails after all retries
   */
  public void executeSQLWithRetry(SQLOperation operation, String threadName, String operationName)
      throws SQLException {
    int attempts = 0;
    SQLException lastException = null;

    while (attempts <= maxRetries) {
      try {
        operation.execute();
        return;
      } catch (SQLException e) {
        attempts++;
        lastException = e;

        if (isDuplicateKeyException(e)) {
          log.warn("Skipping {} due to duplicate key violation", operationName);
          return;
        }

        if (attempts > maxRetries) {
          break;
        }

        log.error(
            "SQL operation {} failed (attempt {}/{}), retrying...",
            operationName,
            attempts,
            maxRetries,
            e);

        handleRetryDelay(attempts);
        handleConnectionRecovery(threadName, e);
      }
    }

    throw new SQLException(
        String.format("Failed to execute %s after %d attempts", operationName, maxRetries),
        lastException);
  }

  /** Checks if an exception is due to a duplicate key violation. */
  private boolean isDuplicateKeyException(Throwable t) {
    if (t == null) {
      return false;
    }

    String message = t.getMessage();
    if (message != null && message.toLowerCase().contains("duplicate")) {
      return true;
    }

    return isDuplicateKeyException(t.getCause());
  }

  /** Handles the retry delay with exponential backoff. */
  private void handleRetryDelay(int attempt) {
    try {
      long delay = TimeUtil.calExpDelay(attempt);
      Thread.sleep(delay);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for retry", ie);
    }
  }

  /** Handles connection recovery after a failure. */
  private void handleConnectionRecovery(String threadName, Exception e) {
    try {
      // Check if this is a connection-related error
      if (isConnectionError(e)) {
        log.info("Refreshing connection for thread {} due to connection error", threadName);
        connectionManager.refreshConnection(threadName);
      }
    } catch (SQLException refreshException) {
      log.error("Failed to refresh connection for thread {}", threadName, refreshException);
    }
  }

  /** Checks if an exception indicates a connection error. */
  private boolean isConnectionError(Exception e) {
    if (e instanceof SQLException) {
      String sqlState = ((SQLException) e).getSQLState();
      // Common SQL states for connection errors
      return sqlState != null
          && (sqlState.startsWith("08")
              || // Connection exception
              sqlState.equals("HY000")
              || // General error (often connection-related)
              sqlState.equals("57P01") // Admin shutdown
          );
    }

    String message = e.getMessage();
    return message != null
        && (message.toLowerCase().contains("connection")
            || message.toLowerCase().contains("closed")
            || message.toLowerCase().contains("timeout"));
  }

  /** Functional interface for SQL operations that can throw SQLException. */
  @FunctionalInterface
  public interface SQLOperation {
    void execute() throws SQLException;
  }
}
