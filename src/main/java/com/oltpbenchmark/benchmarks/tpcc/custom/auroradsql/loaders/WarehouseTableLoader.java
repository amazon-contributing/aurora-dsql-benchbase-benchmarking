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
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.ConnectionManager;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.RetryHandler;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.TPCCLoaderConstants;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Warehouse;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/** Loader for the WAREHOUSE table. */
@Slf4j
public class WarehouseTableLoader extends AbstractTableLoader {

  public WarehouseTableLoader(
      BenchmarkModule benchmark,
      ConnectionManager connectionManager,
      RetryHandler retryHandler,
      int batchSize) {
    super(benchmark, connectionManager, retryHandler, batchSize);
  }

  @Override
  protected String getTableName() {
    return TPCCConstants.TABLENAME_WAREHOUSE;
  }

  @Override
  public void load(String threadName) throws SQLException {
    throw new UnsupportedOperationException("Use load(String threadName, int warehouseId) instead");
  }

  /** Loads a single warehouse. */
  public void load(String threadName, int warehouseId) throws SQLException {
    log.info("Starting to load WAREHOUSE {}", warehouseId);

    loadWarehouse(threadName, warehouseId);

    log.info("Finished loading WAREHOUSE {}", warehouseId);
  }

  private void loadWarehouse(String threadName, int warehouseId) throws SQLException {
    Warehouse warehouse = generateWarehouse(warehouseId);

    executeWithRetry(
        () -> {
          PreparedStatement stmt = getInsertStatement(threadName);
          setWarehouseParameters(stmt, warehouse);
          stmt.execute();
        },
        threadName,
        "Insert Warehouse");
  }

  private Warehouse generateWarehouse(int warehouseId) {
    Warehouse warehouse = new Warehouse();

    warehouse.w_id = warehouseId;
    warehouse.w_ytd = (float) TPCCLoaderConstants.WAREHOUSE_INITIAL_YTD;

    // random within [0.0000 .. 0.2000]
    warehouse.w_tax =
        TPCCUtil.randomNumber(0, TPCCLoaderConstants.WAREHOUSE_TAX_MAX, benchmark.rng()) / 10000.0;
    warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
    warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
    warehouse.w_zip = "123456789";

    return warehouse;
  }

  private void setWarehouseParameters(PreparedStatement stmt, Warehouse warehouse)
      throws SQLException {
    int idx = 1;
    stmt.setLong(idx++, warehouse.w_id);
    stmt.setDouble(idx++, warehouse.w_ytd);
    stmt.setDouble(idx++, warehouse.w_tax);
    stmt.setString(idx++, warehouse.w_name);
    stmt.setString(idx++, warehouse.w_street_1);
    stmt.setString(idx++, warehouse.w_street_2);
    stmt.setString(idx++, warehouse.w_city);
    stmt.setString(idx++, warehouse.w_state);
    stmt.setString(idx, warehouse.w_zip);
  }
}
