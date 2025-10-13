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
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.ConnectionManager;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.RetryHandler;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.TPCCLoaderConstants;
import com.oltpbenchmark.benchmarks.tpcc.pojo.District;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/** Loader for the DISTRICT table. */
@Slf4j
public class DistrictTableLoader extends AbstractTableLoader {

  public DistrictTableLoader(
      BenchmarkModule benchmark,
      ConnectionManager connectionManager,
      RetryHandler retryHandler,
      int batchSize) {
    super(benchmark, connectionManager, retryHandler, batchSize);
  }

  @Override
  protected String getTableName() {
    return TPCCConstants.TABLENAME_DISTRICT;
  }

  @Override
  public void load(String threadName) throws SQLException {
    throw new UnsupportedOperationException("Use load(String threadName, int warehouseId) instead");
  }

  /** Loads districts for a specific warehouse. */
  public void load(String threadName, int warehouseId) throws SQLException {
    log.info("Starting to load DISTRICT for warehouse {}", warehouseId);

    loadDistricts(threadName, warehouseId, TPCCConfig.configDistPerWhse);

    log.info("Finished loading DISTRICT for warehouse {}", warehouseId);
  }

  private void loadDistricts(String threadName, int warehouseId, int districtsPerWarehouse)
      throws SQLException {
    for (int d = 1; d <= districtsPerWarehouse; d++) {
      District district = generateDistrict(warehouseId, d);

      executeWithRetry(
          () -> {
            PreparedStatement stmt = getInsertStatement(threadName);
            setDistrictParameters(stmt, district);
            stmt.executeUpdate();
          },
          threadName,
          "Insert District");
    }
  }

  private District generateDistrict(int warehouseId, int districtId) {
    District district = new District();

    district.d_id = districtId;
    district.d_w_id = warehouseId;
    district.d_ytd = (float) TPCCLoaderConstants.DISTRICT_INITIAL_YTD;

    // random within [0.0000 .. 0.2000]
    district.d_tax =
        (float)
            (TPCCUtil.randomNumber(0, TPCCLoaderConstants.DISTRICT_TAX_MAX, benchmark.rng())
                / 10000.0);

    district.d_next_o_id = TPCCConfig.configCustPerDist + 1;
    district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
    district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    district.d_state = TPCCUtil.randomStr(3).toUpperCase();
    district.d_zip = "123456789";

    return district;
  }

  private void setDistrictParameters(PreparedStatement stmt, District district)
      throws SQLException {
    int idx = 1;
    stmt.setLong(idx++, district.d_w_id);
    stmt.setLong(idx++, district.d_id);
    stmt.setDouble(idx++, district.d_ytd);
    stmt.setDouble(idx++, district.d_tax);
    stmt.setLong(idx++, district.d_next_o_id);
    stmt.setString(idx++, district.d_name);
    stmt.setString(idx++, district.d_street_1);
    stmt.setString(idx++, district.d_street_2);
    stmt.setString(idx++, district.d_city);
    stmt.setString(idx++, district.d_state);
    stmt.setString(idx, district.d_zip);
  }
}
