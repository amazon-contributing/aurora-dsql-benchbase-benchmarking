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

package com.oltpbenchmark.benchmarks.tpcc;

import static java.util.stream.Collectors.joining;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.DSQLTPCCLoader;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.types.DatabaseType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TPCCBenchmark extends BenchmarkModule {
  private static final Logger LOG = LoggerFactory.getLogger(TPCCBenchmark.class);

  public TPCCBenchmark(WorkloadConfiguration workConf) {
    super(workConf);
  }

  @Override
  protected Package getProcedurePackageImpl() {
    return (NewOrder.class.getPackage());
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();

    try {
      List<TPCCWorker> terminals = createTerminals();
      workers.addAll(terminals);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    return workers;
  }

  @Override
  protected Loader<TPCCBenchmark> makeLoaderImpl() {
    if (this.workConf.getDatabaseType() == DatabaseType.AURORADSQL) {
      return new DSQLTPCCLoader(this);
    }
    return new TPCCLoader(this);
  }

  protected List<TPCCWorker> createTerminals() throws SQLException {
    final List<TPCCWorker> workers = createTerminalsOldWay();

    final String assignedWarehouses =
        workers.stream()
            .map(worker -> String.valueOf(worker.getTerminalWarehouseID()))
            .collect(joining(","));

    LOG.info("Created workers for warehouses: {}", assignedWarehouses);

    return workers;
  }

  private List<TPCCWorker> createTerminalsOldWay() throws SQLException {
    TPCCWorker[] terminals = new TPCCWorker[workConf.getTerminals()];

    // totalWarehouses is equal to numWarehouses in case of non-partitioned use case
    int totalWarehouses = (int) workConf.getScaleFactor();
    if (totalWarehouses <= 0) {
      totalWarehouses = 1;
    }

    // [startWarehouseIndex, endWarehouseIndex] are both included.
    // Use defaults if not configured: start=1, end=totalWarehouses, stride=1
    final int startWarehouseIndex =
        workConf.getStartWarehouseIndex() > 0 ? workConf.getStartWarehouseIndex() : 1;
    final int endWarehouseIndex =
        workConf.getEndWarehouseIndex() > 0 ? workConf.getEndWarehouseIndex() : totalWarehouses;
    final int stride = workConf.getStride() > 0 ? workConf.getStride() : 1;

    LOG.info(
        "Start warehouse idx: {} end warehouse idx: {} stride: {}",
        startWarehouseIndex,
        endWarehouseIndex,
        stride);

    final List<Integer> w_ids = new ArrayList<>();
    for (int w_id = startWarehouseIndex; w_id <= endWarehouseIndex; w_id += stride) {
      w_ids.add(w_id);
    }
    final int numWarehouses = w_ids.size();
    int numTerminals = workConf.getTerminals();

    assert startWarehouseIndex >= 1 : "The start index must be >= 1";
    assert endWarehouseIndex >= 1 : "The end index must be >= 1";
    assert endWarehouseIndex <= totalWarehouses
        : "The end index must be within the total warehouse number";
    assert numWarehouses >= 1 : "At least need 1 warehouse to do benchmark";

    // We distribute terminals evenly across the warehouses
    // Eg. if there are 10 terminals across 7 warehouses, they
    // are distributed as
    // 1, 1, 2, 1, 2, 1, 2
    final double terminalsPerWarehouse = (double) numTerminals / numWarehouses;
    int workerId = 0;

    for (int w = 0; w < numWarehouses; w++) {
      // Compute the number of terminals in *this* warehouse
      int lowerTerminalId = (int) (w * terminalsPerWarehouse);
      int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
      // protect against double rounding errors
      if (w + 1 == numWarehouses) {
        upperTerminalId = numTerminals;
      }
      int numWarehouseTerminals = upperTerminalId - lowerTerminalId;
      int w_id = w_ids.get(w);

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            String.format(
                "w_id %d = %d terminals [lower=%d / upper%d]",
                w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));
      }

      final double districtsPerTerminal =
          TPCCConfig.configDistPerWhse / (double) numWarehouseTerminals;
      for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
        int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
        int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
        if (terminalId + 1 == numWarehouseTerminals) {
          upperDistrictId = TPCCConfig.configDistPerWhse;
        }
        lowerDistrictId += 1;
        TPCCWorker terminal =
            new TPCCWorker(
                this, workerId++, w_id, lowerDistrictId, upperDistrictId, totalWarehouses);
        terminals[lowerTerminalId + terminalId] = terminal;
      }
    }

    return Arrays.asList(terminals);
  }
}
