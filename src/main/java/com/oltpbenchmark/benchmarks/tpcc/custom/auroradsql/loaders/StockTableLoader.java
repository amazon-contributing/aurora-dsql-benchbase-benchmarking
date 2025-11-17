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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Stock;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/** Loader for the STOCK table. */
@Slf4j
public class StockTableLoader extends AbstractTableLoader {

  public StockTableLoader(
      BenchmarkModule benchmark,
      ConnectionManager connectionManager,
      RetryHandler retryHandler,
      int batchSize) {
    super(benchmark, connectionManager, retryHandler, batchSize);
  }

  @Override
  protected String getTableName() {
    return TPCCConstants.TABLENAME_STOCK;
  }

  @Override
  public void load(String threadName) throws SQLException {
    throw new UnsupportedOperationException("Use load(String threadName, int warehouseId) instead");
  }

  /** Loads stock for a specific warehouse. */
  public void load(String threadName, int warehouseId) throws SQLException {
    log.info("Starting to load STOCK for warehouse {}", warehouseId);

    loadStock(threadName, warehouseId, TPCCConfig.configItemCount);

    log.info("Finished loading STOCK for warehouse {}", warehouseId);
  }

  private void loadStock(String threadName, int warehouseId, int numItems) throws SQLException {
    BatchProcessor<Stock> batchProcessor =
        new BatchProcessor<>(batchSize, this::setStockParameters);

    for (int i = 1; i <= numItems; i++) {
      Stock stock = generateStock(warehouseId, i);
      batchProcessor.add(stock);

      // Flush when batch is full
      if (batchProcessor.shouldFlush()) {
        executeWithRetry(
            () -> {
              PreparedStatement stmt = getInsertStatement(threadName);
              batchProcessor.flush(stmt);
            },
            threadName,
            "Flush stocks batch");
      }
    }

    // Flush any remaining stocks
    executeWithRetry(
        () -> {
          PreparedStatement stmt = getInsertStatement(threadName);
          batchProcessor.flush(stmt);
        },
        threadName,
        "Flush remaining stocks");
  }

  private Stock generateStock(int warehouseId, int itemId) {
    Stock stock = new Stock();

    stock.s_i_id = itemId;
    stock.s_w_id = warehouseId;
    stock.s_quantity =
        TPCCUtil.randomNumber(
            TPCCLoaderConstants.STOCK_QUANTITY_MIN,
            TPCCLoaderConstants.STOCK_QUANTITY_MAX,
            benchmark.rng());
    stock.s_ytd = 0;
    stock.s_order_cnt = 0;
    stock.s_remote_cnt = 0;

    // Generate s_data (similar to item data)
    stock.s_data = generateStockData();

    return stock;
  }

  private String generateStockData() {
    int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
    int len =
        TPCCUtil.randomNumber(
            TPCCLoaderConstants.ITEM_DATA_MIN_LENGTH,
            TPCCLoaderConstants.ITEM_DATA_MAX_LENGTH,
            benchmark.rng());

    if (randPct > TPCCLoaderConstants.ORIGINAL_DATA_THRESHOLD) {
      // 90% of time s_data is a random string
      return TPCCUtil.randomStr(len);
    } else {
      // 10% of time s_data has "ORIGINAL" in the middle
      int startOriginal = TPCCUtil.randomNumber(2, len - 8, benchmark.rng());
      return TPCCUtil.randomStr(startOriginal - 1)
          + TPCCLoaderConstants.ORIGINAL_STRING
          + TPCCUtil.randomStr(len - startOriginal - 9);
    }
  }

  private void setStockParameters(PreparedStatement stmt, Stock stock) {
    try {
      int idx = 1;
      stmt.setLong(idx++, stock.s_w_id);
      stmt.setLong(idx++, stock.s_i_id);
      stmt.setLong(idx++, stock.s_quantity);
      stmt.setDouble(idx++, stock.s_ytd);
      stmt.setLong(idx++, stock.s_order_cnt);
      stmt.setLong(idx++, stock.s_remote_cnt);
      stmt.setString(idx++, stock.s_data);

      // Set 10 district fields (s_dist_01 through s_dist_10)
      for (int i = 0; i < 10; i++) {
        stmt.setString(idx++, TPCCUtil.randomStr(24));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to set stock parameters", e);
    }
  }
}
