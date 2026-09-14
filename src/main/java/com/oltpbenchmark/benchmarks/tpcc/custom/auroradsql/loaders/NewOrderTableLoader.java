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
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.BatchProcessor;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.ConnectionManager;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.RetryHandler;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.TPCCLoaderConstants;
import com.oltpbenchmark.benchmarks.tpcc.pojo.NewOrder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/**
 * Loader for the NEW_ORDER table.
 *
 * <p>New orders are only created for unprocessed orders (o_id >= FIRST_UNPROCESSED_O_ID).
 */
@Slf4j
public class NewOrderTableLoader extends AbstractTableLoader {

  public NewOrderTableLoader(
      BenchmarkModule benchmark,
      ConnectionManager connectionManager,
      RetryHandler retryHandler,
      int batchSize) {
    super(benchmark, connectionManager, retryHandler, batchSize);
  }

  @Override
  protected String getTableName() {
    return TPCCConstants.TABLENAME_NEWORDER;
  }

  @Override
  public void load(String threadName) throws SQLException {
    throw new UnsupportedOperationException("Use load(String threadName, int warehouseId) instead");
  }

  /**
   * Loads new orders for a specific warehouse.
   *
   * <p>According to TPC-C specification, new orders are only created for orders with o_id >=
   * FIRST_UNPROCESSED_O_ID (2101).
   */
  public void load(String threadName, int warehouseId) throws SQLException {
    log.info("Starting to load NEW_ORDER for warehouse {}", warehouseId);

    loadNewOrders(
        threadName, warehouseId, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

    log.info("Finished loading NEW_ORDER for warehouse {}", warehouseId);
  }

  private void loadNewOrders(
      String threadName, int warehouseId, int districtsPerWarehouse, int customersPerDistrict)
      throws SQLException {
    BatchProcessor<NewOrder> batchProcessor =
        new BatchProcessor<>(batchSize, this::setNewOrderParameters);

    for (int d = 1; d <= districtsPerWarehouse; d++) {
      for (int c = 1; c <= customersPerDistrict; c++) {
        // New orders are only created for unprocessed orders
        if (c >= TPCCLoaderConstants.FIRST_UNPROCESSED_O_ID) {
          NewOrder newOrder = new NewOrder();
          newOrder.no_w_id = warehouseId;
          newOrder.no_d_id = d;
          newOrder.no_o_id = c;

          batchProcessor.add(newOrder);

          // Flush when batch is full
          if (batchProcessor.shouldFlush()) {
            executeWithRetry(
                () -> {
                  PreparedStatement stmt = getInsertStatement(threadName);
                  batchProcessor.flush(stmt);
                },
                threadName,
                "Flush new orders batch");
          }
        }
      }
    }

    // Flush any remaining new orders
    executeWithRetry(
        () -> {
          PreparedStatement stmt = getInsertStatement(threadName);
          batchProcessor.flush(stmt);
        },
        threadName,
        "Flush remaining new orders");
  }

  private void setNewOrderParameters(PreparedStatement stmt, NewOrder newOrder) {
    try {
      int idx = 1;
      stmt.setInt(idx++, newOrder.no_w_id);
      stmt.setInt(idx++, newOrder.no_d_id);
      stmt.setInt(idx, newOrder.no_o_id);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to set new order parameters", e);
    }
  }
}
