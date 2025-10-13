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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Item;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/** Loader for the ITEM table. */
@Slf4j
public class ItemTableLoader extends AbstractTableLoader {

  public ItemTableLoader(
      BenchmarkModule benchmark,
      ConnectionManager connectionManager,
      RetryHandler retryHandler,
      int batchSize) {
    super(benchmark, connectionManager, retryHandler, batchSize);
  }

  @Override
  protected String getTableName() {
    return TPCCConstants.TABLENAME_ITEM;
  }

  @Override
  public void load(String threadName) throws SQLException {
    log.info("Starting to load ITEM table");

    loadItems(threadName, TPCCConfig.configItemCount);

    log.info("Finished loading ITEM table");
  }

  private void loadItems(String threadName, int itemCount) throws SQLException {
    BatchProcessor<Item> batchProcessor = new BatchProcessor<>(batchSize, this::setItemParameters);

    for (int i = 1; i <= itemCount; i++) {
      Item item = generateItem(i);

      executeWithRetry(
          () -> {
            PreparedStatement stmt = getInsertStatement(threadName);
            batchProcessor.add(item, stmt);
          },
          threadName,
          "Insert Item");
    }

    // Flush any remaining items
    executeWithRetry(
        () -> {
          PreparedStatement stmt = getInsertStatement(threadName);
          batchProcessor.flush(stmt);
        },
        threadName,
        "Flush remaining items");
  }

  private Item generateItem(int itemId) {
    Item item = new Item();
    item.i_id = itemId;
    item.i_name =
        TPCCUtil.randomStr(
            TPCCUtil.randomNumber(
                TPCCLoaderConstants.ITEM_NAME_MIN_LENGTH,
                TPCCLoaderConstants.ITEM_NAME_MAX_LENGTH,
                benchmark.rng()));
    item.i_price =
        TPCCUtil.randomNumber(
                TPCCLoaderConstants.ITEM_PRICE_MIN,
                TPCCLoaderConstants.ITEM_PRICE_MAX,
                benchmark.rng())
            / 100.0;

    // Generate i_data
    item.i_data = generateItemData();
    item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

    return item;
  }

  private String generateItemData() {
    int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
    int len =
        TPCCUtil.randomNumber(
            TPCCLoaderConstants.ITEM_DATA_MIN_LENGTH,
            TPCCLoaderConstants.ITEM_DATA_MAX_LENGTH,
            benchmark.rng());

    if (randPct > TPCCLoaderConstants.ORIGINAL_DATA_THRESHOLD) {
      // 90% of time i_data is a random string
      return TPCCUtil.randomStr(len);
    } else {
      // 10% of time i_data has "ORIGINAL" in the middle
      int startOriginal = TPCCUtil.randomNumber(2, len - 8, benchmark.rng());
      return TPCCUtil.randomStr(startOriginal - 1)
          + TPCCLoaderConstants.ORIGINAL_STRING
          + TPCCUtil.randomStr(len - startOriginal - 9);
    }
  }

  private void setItemParameters(PreparedStatement stmt, Item item) {
    try {
      int idx = 1;
      stmt.setLong(idx++, item.i_id);
      stmt.setString(idx++, item.i_name);
      stmt.setDouble(idx++, item.i_price);
      stmt.setString(idx++, item.i_data);
      stmt.setLong(idx, item.i_im_id);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to set item parameters", e);
    }
  }
}
