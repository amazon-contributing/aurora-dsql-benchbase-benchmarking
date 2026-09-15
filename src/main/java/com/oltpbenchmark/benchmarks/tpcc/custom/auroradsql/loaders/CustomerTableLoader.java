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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;

/** Loader for the CUSTOMER table. */
@Slf4j
public class CustomerTableLoader extends AbstractTableLoader {

  public CustomerTableLoader(
      BenchmarkModule benchmark,
      ConnectionManager connectionManager,
      RetryHandler retryHandler,
      int batchSize) {
    super(benchmark, connectionManager, retryHandler, batchSize);
  }

  @Override
  protected String getTableName() {
    return TPCCConstants.TABLENAME_CUSTOMER;
  }

  @Override
  public void load(String threadName) throws SQLException {
    throw new UnsupportedOperationException("Use load(String threadName, int warehouseId) instead");
  }

  /** Loads customers for a specific warehouse. */
  public void load(String threadName, int warehouseId) throws SQLException {
    log.info("Starting to load CUSTOMER for warehouse {}", warehouseId);
    loadCustomers(
        threadName, warehouseId, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
    log.info("Finished loading CUSTOMER for warehouse {}", warehouseId);
  }

  private void loadCustomers(
      String threadName, int warehouseId, int districtsPerWarehouse, int customersPerDistrict)
      throws SQLException {
    BatchProcessor<Customer> batchProcessor =
        new BatchProcessor<>(batchSize, this::setCustomerParameters);

    for (int d = 1; d <= districtsPerWarehouse; d++) {
      for (int c = 1; c <= customersPerDistrict; c++) {
        Customer customer = generateCustomer(warehouseId, d, c);
        batchProcessor.add(customer);

        // Flush when batch is full
        if (batchProcessor.shouldFlush()) {
          executeWithRetry(
              () -> {
                PreparedStatement stmt = getInsertStatement(threadName);
                batchProcessor.flush(stmt);
              },
              threadName,
              "Flush customers batch");
        }
      }
    }

    // Flush any remaining customers
    executeWithRetry(
        () -> {
          PreparedStatement stmt = getInsertStatement(threadName);
          batchProcessor.flush(stmt);
        },
        threadName,
        "Flush remaining customers");
  }

  private Customer generateCustomer(int warehouseId, int districtId, int customerId) {
    Customer customer = new Customer();
    Timestamp sysdate = new Timestamp(System.currentTimeMillis());

    customer.c_id = customerId;
    customer.c_d_id = districtId;
    customer.c_w_id = warehouseId;

    // discount is random between [0.0000 ... 0.5000]
    customer.c_discount =
        (float)
            (TPCCUtil.randomNumber(1, TPCCLoaderConstants.CUSTOMER_DISCOUNT_MAX, benchmark.rng())
                / 10000.0);

    // 10% Bad Credit, 90% Good Credit
    if (TPCCUtil.randomNumber(1, 100, benchmark.rng())
        <= TPCCLoaderConstants.BAD_CREDIT_THRESHOLD) {
      customer.c_credit = TPCCLoaderConstants.BAD_CREDIT;
    } else {
      customer.c_credit = TPCCLoaderConstants.GOOD_CREDIT;
    }

    // Last name handling - first 1000 customers have special last names
    if (customerId <= 1000) {
      customer.c_last = TPCCUtil.getLastName(customerId - 1);
    } else {
      customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(benchmark.rng());
    }

    customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, benchmark.rng()));
    customer.c_credit_lim = (float) TPCCLoaderConstants.CUSTOMER_CREDIT_LIMIT;

    customer.c_balance = (float) TPCCLoaderConstants.CUSTOMER_INITIAL_BALANCE;
    customer.c_ytd_payment = (float) TPCCLoaderConstants.CUSTOMER_INITIAL_YTD_PAYMENT;
    customer.c_payment_cnt = TPCCLoaderConstants.CUSTOMER_INITIAL_PAYMENT_CNT;
    customer.c_delivery_cnt = TPCCLoaderConstants.CUSTOMER_INITIAL_DELIVERY_CNT;

    customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    customer.c_state = TPCCUtil.randomStr(3).toUpperCase();

    // TPC-C 4.3.2.7: 4 random digits + "11111"
    customer.c_zip = TPCCUtil.randomNStr(4) + TPCCLoaderConstants.ZIP_SUFFIX;
    customer.c_phone = TPCCUtil.randomNStr(16);
    customer.c_since = sysdate;
    customer.c_middle = TPCCLoaderConstants.MIDDLE_NAME;
    customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, benchmark.rng()));

    return customer;
  }

  private void setCustomerParameters(PreparedStatement stmt, Customer customer) {
    try {
      int idx = 1;
      stmt.setLong(idx++, customer.c_w_id);
      stmt.setLong(idx++, customer.c_d_id);
      stmt.setLong(idx++, customer.c_id);
      stmt.setDouble(idx++, customer.c_discount);
      stmt.setString(idx++, customer.c_credit);
      stmt.setString(idx++, customer.c_last);
      stmt.setString(idx++, customer.c_first);
      stmt.setDouble(idx++, customer.c_credit_lim);
      stmt.setDouble(idx++, customer.c_balance);
      stmt.setDouble(idx++, customer.c_ytd_payment);
      stmt.setLong(idx++, (long) customer.c_payment_cnt);
      stmt.setLong(idx++, (long) customer.c_delivery_cnt);
      stmt.setString(idx++, customer.c_street_1);
      stmt.setString(idx++, customer.c_street_2);
      stmt.setString(idx++, customer.c_city);
      stmt.setString(idx++, customer.c_state);
      stmt.setString(idx++, customer.c_zip);
      stmt.setString(idx++, customer.c_phone);
      stmt.setTimestamp(idx++, customer.c_since);
      stmt.setString(idx++, customer.c_middle);
      stmt.setString(idx, customer.c_data);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to set customer parameters", e);
    }
  }
}
