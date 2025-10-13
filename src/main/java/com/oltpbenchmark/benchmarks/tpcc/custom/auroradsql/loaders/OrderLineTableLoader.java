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
import com.oltpbenchmark.benchmarks.tpcc.pojo.OrderLine;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

/**
 * Loader for the ORDER_LINE table.
 *
 * <p>Order lines are created for each order, with the number of lines determined by the order's
 * line count.
 */
@Slf4j
public class OrderLineTableLoader extends AbstractTableLoader {

  public OrderLineTableLoader(
      BenchmarkModule benchmark,
      ConnectionManager connectionManager,
      RetryHandler retryHandler,
      int batchSize) {
    super(benchmark, connectionManager, retryHandler, batchSize);
  }

  @Override
  protected String getTableName() {
    return TPCCConstants.TABLENAME_ORDERLINE;
  }

  @Override
  public void load(String threadName) throws SQLException {
    throw new UnsupportedOperationException("Use load(String threadName, int warehouseId) instead");
  }

  /** Loads order lines for a specific warehouse. */
  public void load(String threadName, int warehouseId) throws SQLException {
    log.info("Starting to load ORDER_LINE for warehouse {}", warehouseId);

    loadOrderLines(
        threadName, warehouseId, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

    log.info("Finished loading ORDER_LINE for warehouse {}", warehouseId);
  }

  private void loadOrderLines(
      String threadName, int warehouseId, int districtsPerWarehouse, int customersPerDistrict)
      throws SQLException {
    BatchProcessor<OrderLine> batchProcessor =
        new BatchProcessor<>(batchSize, this::setOrderLineParameters);

    for (int d = 1; d <= districtsPerWarehouse; d++) {
      for (int c = 1; c <= customersPerDistrict; c++) {
        int orderLineCount = getOrderLineCount(warehouseId, c, d);

        for (int l = 1; l <= orderLineCount; l++) {
          OrderLine orderLine = generateOrderLine(warehouseId, d, c, l);

          executeWithRetry(
              () -> {
                PreparedStatement stmt = getInsertStatement(threadName);
                batchProcessor.add(orderLine, stmt);
              },
              threadName,
              "Insert Order Line");
        }
      }
    }

    // Flush any remaining order lines
    executeWithRetry(
        () -> {
          PreparedStatement stmt = getInsertStatement(threadName);
          batchProcessor.flush(stmt);
        },
        threadName,
        "Flush remaining order lines");
  }

  private OrderLine generateOrderLine(
      int warehouseId, int districtId, int orderId, int lineNumber) {
    OrderLine orderLine = new OrderLine();

    orderLine.ol_w_id = warehouseId;
    orderLine.ol_d_id = districtId;
    orderLine.ol_o_id = orderId;
    orderLine.ol_number = lineNumber;
    orderLine.ol_i_id = TPCCUtil.randomNumber(1, TPCCConfig.configItemCount, benchmark.rng());

    // Set delivery date and amount based on whether order is processed
    if (orderId < TPCCLoaderConstants.FIRST_UNPROCESSED_O_ID) {
      // Processed order
      orderLine.ol_delivery_d = new Timestamp(System.currentTimeMillis());
      orderLine.ol_amount = 0;
    } else {
      // Unprocessed order
      orderLine.ol_delivery_d = null;
      // Random amount within [0.01 .. 9,999.99]
      orderLine.ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
    }

    orderLine.ol_supply_w_id = orderLine.ol_w_id;
    orderLine.ol_quantity = TPCCLoaderConstants.ORDER_LINE_QUANTITY;
    orderLine.ol_dist_info = TPCCUtil.randomStr(24);

    return orderLine;
  }

  private int getOrderLineCount(int warehouseId, int orderId, int districtId) {
    // Use a deterministic random based on customer info for consistency
    // This ensures the same order always has the same number of order lines
    Customer customer = new Customer();
    customer.c_id = orderId;
    customer.c_d_id = districtId;
    customer.c_w_id = warehouseId;

    Random random = new Random(customer.hashCode());
    return TPCCUtil.randomNumber(
        TPCCLoaderConstants.ORDER_LINE_COUNT_MIN, TPCCLoaderConstants.ORDER_LINE_COUNT_MAX, random);
  }

  private void setOrderLineParameters(PreparedStatement stmt, OrderLine orderLine) {
    try {
      int idx = 1;
      stmt.setInt(idx++, orderLine.ol_w_id);
      stmt.setInt(idx++, orderLine.ol_d_id);
      stmt.setInt(idx++, orderLine.ol_o_id);
      stmt.setInt(idx++, orderLine.ol_number);
      stmt.setInt(idx++, orderLine.ol_i_id);

      if (orderLine.ol_delivery_d != null) {
        stmt.setTimestamp(idx++, orderLine.ol_delivery_d);
      } else {
        stmt.setNull(idx++, Types.TIMESTAMP);
      }

      stmt.setFloat(idx++, orderLine.ol_amount);
      stmt.setInt(idx++, orderLine.ol_supply_w_id);
      stmt.setInt(idx++, orderLine.ol_quantity);
      stmt.setString(idx, orderLine.ol_dist_info);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to set order line parameters", e);
    }
  }
}
