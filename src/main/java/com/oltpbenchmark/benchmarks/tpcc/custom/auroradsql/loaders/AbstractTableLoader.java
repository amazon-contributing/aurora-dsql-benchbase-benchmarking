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

package com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.ConnectionManager;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.RetryHandler;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.SQLUtil;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract base class for all table loaders. Provides common functionality for loading data into
 * TPCC tables.
 */
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractTableLoader {

  protected final BenchmarkModule benchmark;
  protected final ConnectionManager connectionManager;
  protected final RetryHandler retryHandler;
  protected final int batchSize;

  /** Gets the table name for this loader. */
  protected abstract String getTableName();

  /** Loads data for this table. */
  public abstract void load(String threadName) throws SQLException;

  /** Gets or creates a prepared statement for inserting into this table. */
  protected PreparedStatement getInsertStatement(String threadName) throws SQLException {
    Table catalogTable = benchmark.getCatalog().getTable(getTableName());
    String sql = SQLUtil.getInsertSQL(catalogTable, getDatabaseType());
    return connectionManager.getPreparedStatement(threadName, getTableName(), sql);
  }

  /** Gets the database type from the benchmark. */
  protected DatabaseType getDatabaseType() {
    return benchmark.getWorkloadConfiguration().getDatabaseType();
  }

  /** Executes an operation with retry logic. */
  protected void executeWithRetry(
      RetryHandler.SQLOperation operation, String threadName, String operationName)
      throws SQLException {
    retryHandler.executeSQLWithRetry(operation, threadName, operationName);
  }

  /** Cleans up resources for this loader. */
  public void cleanup(String threadName) {
    connectionManager.closePreparedStatement(threadName, getTableName());
  }
}
