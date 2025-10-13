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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

/** Loader for the OORDER (open order) table. */
@Slf4j
public class OrderTableLoader extends AbstractTableLoader {

  public OrderTableLoader(
      BenchmarkModule benchmark,
      ConnectionManager connectionManager,
      RetryHandler retryHandler,
      int batchSize) {
    super(benchmark, connectionManager, retryHandler, batchSize);
  }

  @Override
  protected String getTableName() {
    return TPCCConstants.TABLENAME_OPENORDER;
  }

  @Override
  public void load(String threadName) throws SQLException {
    throw new UnsupportedOperationException("Use load(String threadName, int warehouseId) instead");
  }

  /** Loads open orders for a specific warehouse. */
  public void load(String threadName, int warehouseId) throws SQLException {
    log.info("Starting to load OORDER for warehouse {}", warehouseId);

    loadOpenOrders(
        threadName, warehouseId, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

    log.info("Finished loading OORDER for warehouse {}", warehouseId);
  }

  private void loadOpenOrders(
      String threadName, int warehouseId, int districtsPerWarehouse, int customersPerDistrict)
      throws SQLException {
    BatchProcessor<Oorder> batchProcessor =
        new BatchProcessor<>(batchSize, this::setOrderParameters);

    for (int d = 1; d <= districtsPerWarehouse; d++) {
      // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
      int[] c_ids = generateCustomerIdPermutation(customersPerDistrict);

      for (int c = 1; c <= customersPerDistrict; c++) {
        Oorder order = generateOrder(warehouseId, d, c, c_ids[c - 1]);

        executeWithRetry(
            () -> {
              PreparedStatement stmt = getInsertStatement(threadName);
              batchProcessor.add(order, stmt);
            },
            threadName,
            "Insert Order");
      }
    }

    // Flush any remaining orders
    executeWithRetry(
        () -> {
          PreparedStatement stmt = getInsertStatement(threadName);
          batchProcessor.flush(stmt);
        },
        threadName,
        "Flush remaining orders");
  }

  private int[] generateCustomerIdPermutation(int customersPerDistrict) {
    int[] c_ids = new int[customersPerDistrict];
    for (int i = 0; i < customersPerDistrict; i++) {
      c_ids[i] = i + 1;
    }

    // Fisher-Yates shuffle
    for (int i = 0; i < c_ids.length - 1; i++) {
      int remaining = c_ids.length - i - 1;
      int swapIndex = benchmark.rng().nextInt(remaining) + i + 1;

      int temp = c_ids[swapIndex];
      c_ids[swapIndex] = c_ids[i];
      c_ids[i] = temp;
    }

    return c_ids;
  }

  private Oorder generateOrder(int warehouseId, int districtId, int orderId, int customerId) {
    Oorder order = new Oorder();

    order.o_id = orderId;
    order.o_w_id = warehouseId;
    order.o_d_id = districtId;
    order.o_c_id = customerId;

    // o_carrier_id is set *only* for orders with ids < 2101 [4.3.3.1]
    if (order.o_id < TPCCLoaderConstants.FIRST_UNPROCESSED_O_ID) {
      order.o_carrier_id =
          TPCCUtil.randomNumber(
              TPCCLoaderConstants.CARRIER_ID_MIN,
              TPCCLoaderConstants.CARRIER_ID_MAX,
              benchmark.rng());
    } else {
      order.o_carrier_id = null;
    }

    order.o_ol_cnt = getRandomOrderLineCount(warehouseId, orderId, districtId);
    order.o_all_local = 1;
    order.o_entry_d = new Timestamp(System.currentTimeMillis());

    return order;
  }

  private int getRandomOrderLineCount(int warehouseId, int orderId, int districtId) {
    // Use a deterministic random based on customer info for consistency
    Customer customer = new Customer();
    customer.c_id = orderId;
    customer.c_d_id = districtId;
    customer.c_w_id = warehouseId;

    Random random = new Random(customer.hashCode());
    return TPCCUtil.randomNumber(
        TPCCLoaderConstants.ORDER_LINE_COUNT_MIN, TPCCLoaderConstants.ORDER_LINE_COUNT_MAX, random);
  }

  private void setOrderParameters(PreparedStatement stmt, Oorder order) {
    try {
      int idx = 1;
      stmt.setInt(idx++, order.o_w_id);
      stmt.setInt(idx++, order.o_d_id);
      stmt.setInt(idx++, order.o_id);
      stmt.setInt(idx++, order.o_c_id);

      if (order.o_carrier_id != null) {
        stmt.setInt(idx++, order.o_carrier_id);
      } else {
        stmt.setNull(idx++, Types.INTEGER);
      }

      stmt.setInt(idx++, order.o_ol_cnt);
      stmt.setInt(idx++, order.o_all_local);
      stmt.setTimestamp(idx, order.o_entry_d);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to set order parameters", e);
    }
  }
}
