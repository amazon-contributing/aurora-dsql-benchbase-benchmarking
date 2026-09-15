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
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.BatchProcessor;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.ConnectionManager;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.RetryHandler;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.TPCCLoaderConstants;
import com.oltpbenchmark.benchmarks.tpcc.pojo.History;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;

/** Loader for the HISTORY table. */
@Slf4j
public class HistoryTableLoader extends AbstractTableLoader {

  public HistoryTableLoader(
      BenchmarkModule benchmark,
      ConnectionManager connectionManager,
      RetryHandler retryHandler,
      int batchSize) {
    super(benchmark, connectionManager, retryHandler, batchSize);
  }

  @Override
  protected String getTableName() {
    return TPCCConstants.TABLENAME_HISTORY;
  }

  @Override
  public void load(String threadName) throws SQLException {
    throw new UnsupportedOperationException("Use load(String threadName, int warehouseId) instead");
  }

  /** Loads customer history for a specific warehouse. */
  public void load(String threadName, int warehouseId) throws SQLException {
    log.info("Starting to load HISTORY for warehouse {}", warehouseId);

    loadCustomerHistory(
        threadName, warehouseId, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

    log.info("Finished loading HISTORY for warehouse {}", warehouseId);
  }

  private void loadCustomerHistory(
      String threadName, int warehouseId, int districtsPerWarehouse, int customersPerDistrict)
      throws SQLException {
    BatchProcessor<History> batchProcessor =
        new BatchProcessor<>(batchSize, this::setHistoryParameters);

    for (int d = 1; d <= districtsPerWarehouse; d++) {
      for (int c = 1; c <= customersPerDistrict; c++) {
        History history = generateHistory(warehouseId, d, c);
        batchProcessor.add(history);

        // Flush when batch is full
        if (batchProcessor.shouldFlush()) {
          executeWithRetry(
              () -> {
                PreparedStatement stmt = getInsertStatement(threadName);
                batchProcessor.flush(stmt);
              },
              threadName,
              "Flush history batch");
        }
      }
    }

    // Flush any remaining history records
    executeWithRetry(
        () -> {
          PreparedStatement stmt = getInsertStatement(threadName);
          batchProcessor.flush(stmt);
        },
        threadName,
        "Flush remaining history");
  }

  private History generateHistory(int warehouseId, int districtId, int customerId) {
    History history = new History();
    Timestamp sysdate = new Timestamp(System.currentTimeMillis());

    history.h_c_id = customerId;
    history.h_c_d_id = districtId;
    history.h_c_w_id = warehouseId;
    history.h_d_id = districtId;
    history.h_w_id = warehouseId;
    history.h_date = sysdate;
    history.h_amount = (float) TPCCLoaderConstants.HISTORY_AMOUNT;
    history.h_data =
        TPCCUtil.randomStr(
            TPCCUtil.randomNumber(
                TPCCLoaderConstants.HISTORY_DATA_MIN_LENGTH,
                TPCCLoaderConstants.HISTORY_DATA_MAX_LENGTH,
                benchmark.rng()));

    return history;
  }

  private void setHistoryParameters(PreparedStatement stmt, History history) {
    try {
      int idx = 1;
      stmt.setInt(idx++, history.h_c_id);
      stmt.setInt(idx++, history.h_c_d_id);
      stmt.setInt(idx++, history.h_c_w_id);
      stmt.setInt(idx++, history.h_d_id);
      stmt.setInt(idx++, history.h_w_id);
      stmt.setTimestamp(idx++, history.h_date);
      stmt.setDouble(idx++, history.h_amount);
      stmt.setString(idx, history.h_data);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to set history parameters", e);
    }
  }
}
