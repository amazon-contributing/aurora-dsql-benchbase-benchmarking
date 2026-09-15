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

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.util.ConnectionUtil;
import com.oltpbenchmark.util.Pair;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages database connections and prepared statements for TPCC loader threads. Handles connection
 * lifecycle, statement caching, and session duration management.
 */
@Slf4j
public class ConnectionManager {
  private static final long SESSION_DURATION = Duration.ofMinutes(59).toMillis();

  private final BenchmarkModule benchmark;
  private final ConcurrentMap<String, Pair<Connection, Long>> connections;
  private final ConcurrentMap<String, ConcurrentMap<String, PreparedStatement>> statementsByThread;

  public ConnectionManager(BenchmarkModule benchmark) {
    this.benchmark = benchmark;
    this.connections = new ConcurrentHashMap<>();
    this.statementsByThread = new ConcurrentHashMap<>();
  }

  /**
   * Gets or creates a connection for the given thread. Automatically refreshes connections that
   * have exceeded the session duration.
   */
  public Connection getConnection(String threadName) throws SQLException {
    Pair<Connection, Long> connectionPair = connections.get(threadName);

    if (connectionPair == null || isConnectionExpired(connectionPair)) {
      refreshConnection(threadName);
      connectionPair = connections.get(threadName);
    }

    return connectionPair.first;
  }

  /** Creates a new connection for the thread and stores it. */
  public void createConnection(String threadName) throws SQLException {
    Connection conn = ConnectionUtil.makeConnectionWithRetry(benchmark);
    connections.put(threadName, Pair.of(conn, System.currentTimeMillis()));
  }

  /** Refreshes the connection for a thread, closing the old one if it exists. */
  public void refreshConnection(String threadName) throws SQLException {
    // Clear all statements for this thread FIRST
    ConcurrentMap<String, PreparedStatement> threadStatements =
        statementsByThread.remove(threadName);

    if (threadStatements != null) {
      threadStatements.forEach(
          (tableName, stmt) -> {
            try {
              stmt.close();
            } catch (SQLException e) {
              log.error("Failed to close statement for {}/{}", threadName, tableName, e);
            }
          });
    }

    // Then refresh connection
    closeConnectionForThread(threadName);
    createConnection(threadName);
  }

  /** Gets or creates a prepared statement for the given thread and table. */
  public PreparedStatement getPreparedStatement(String threadName, String tableName, String sql)
      throws SQLException {
    ConcurrentMap<String, PreparedStatement> threadStatements =
        statementsByThread.computeIfAbsent(threadName, k -> new ConcurrentHashMap<>());

    PreparedStatement stmt = threadStatements.get(tableName);

    if (stmt == null || stmt.isClosed()) {
      Connection conn = getConnection(threadName);
      stmt = conn.prepareStatement(sql);
      threadStatements.put(tableName, stmt);
    }

    return stmt;
  }

  /** Closes a specific prepared statement. */
  public void closePreparedStatement(String threadName, String tableName) {
    ConcurrentMap<String, PreparedStatement> threadStatements = statementsByThread.get(threadName);

    if (threadStatements != null) {
      PreparedStatement stmt = threadStatements.remove(tableName);
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          log.error("Failed to close PreparedStatement for {}/{}", threadName, tableName, e);
        }
      }
    }
  }

  /** Closes all resources for a specific thread. */
  public void closeResourcesForThread(String threadName) {
    // Close all statements for this thread
    ConcurrentMap<String, PreparedStatement> threadStatements =
        statementsByThread.remove(threadName);

    if (threadStatements != null) {
      threadStatements.forEach(
          (tableName, stmt) -> {
            try {
              stmt.close();
            } catch (SQLException e) {
              log.error("Failed to close statement for {}/{}", threadName, tableName, e);
            }
          });
    }

    // Close connection
    closeConnectionForThread(threadName);
  }

  /** Closes all connections and statements. */
  public void closeAll() {
    // Close all statements
    statementsByThread.forEach(
        (threadName, threadStatements) -> {
          threadStatements.forEach(
              (tableName, stmt) -> {
                try {
                  stmt.close();
                } catch (SQLException e) {
                  log.error("Failed to close statement for {}/{}", threadName, tableName, e);
                }
              });
        });
    statementsByThread.clear();

    // Close all connections
    connections.forEach(
        (threadName, pair) -> {
          try {
            if (pair.first != null && !pair.first.isClosed()) {
              pair.first.close();
            }
          } catch (SQLException e) {
            log.error("Failed to close connection for thread: {}", threadName, e);
          }
        });
    connections.clear();
  }

  private void closeConnectionForThread(String threadName) {
    Pair<Connection, Long> connectionPair = connections.remove(threadName);
    if (connectionPair != null && connectionPair.first != null) {
      try {
        if (!connectionPair.first.isClosed()) {
          connectionPair.first.close();
        }
      } catch (SQLException e) {
        log.error("Failed to close connection for thread: {}", threadName, e);
      }
    }
  }

  private boolean isConnectionExpired(Pair<Connection, Long> connectionPair) {
    return System.currentTimeMillis() - connectionPair.second >= SESSION_DURATION;
  }
}
