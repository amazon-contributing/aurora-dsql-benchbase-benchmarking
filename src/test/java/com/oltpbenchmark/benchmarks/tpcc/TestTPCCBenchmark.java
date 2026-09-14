/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpcc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class TestTPCCBenchmark extends AbstractTestBenchmarkModule<TPCCBenchmark> {

  public static final List<Class<? extends Procedure>> PROCEDURE_CLASSES =
      List.of(Delivery.class, NewOrder.class, OrderStatus.class, Payment.class, StockLevel.class);

  @Override
  public List<Class<? extends Procedure>> procedures() {
    return TestTPCCBenchmark.PROCEDURE_CLASSES;
  }

  @Override
  public Class<TPCCBenchmark> benchmarkClass() {
    return TPCCBenchmark.class;
  }

  /**
   * Test that workers are created with correct warehouse IDs when using startWarehouseIndex,
   * endWarehouseIndex, and stride parameters for distributed benchmarking.
   */
  @Test
  public void testCreateTerminalsWithStrideDistribution() throws Exception {
    // Configure for distributed benchmarking: warehouses 3, 6, 9 (stride=3, start=3, end=9)
    int scaleFactor = 10;
    int startWarehouseIndex = 3;
    int endWarehouseIndex = 9;
    int stride = 3;
    int terminals = 6;

    this.workConf.setScaleFactor(scaleFactor);
    this.workConf.setStartWarehouseIndex(startWarehouseIndex);
    this.workConf.setEndWarehouseIndex(endWarehouseIndex);
    this.workConf.setStride(stride);
    this.workConf.setTerminals(terminals);

    // Create workers
    List<Worker<? extends BenchmarkModule>> workers = this.benchmark.makeWorkers();

    assertEquals("Should create correct number of workers", terminals, workers.size());

    // Collect all warehouse IDs assigned to workers
    Set<Integer> assignedWarehouseIds = new HashSet<>();
    for (Worker<? extends BenchmarkModule> worker : workers) {
      TPCCWorker tpccWorker = (TPCCWorker) worker;
      assignedWarehouseIds.add(tpccWorker.getTerminalWarehouseID());
    }

    // Expected warehouses: 3, 6, 9 (start=3, end=9, stride=3)
    Set<Integer> expectedWarehouseIds = Set.of(3, 6, 9);

    assertEquals(
        "Workers should only be assigned to warehouses matching stride pattern",
        expectedWarehouseIds,
        assignedWarehouseIds);
  }

  /**
   * Test that terminals are evenly distributed across warehouses. With 10 terminals across 3
   * warehouses, distribution should be approximately 3, 3, 4 (or similar even split).
   */
  @Test
  public void testTerminalsEvenlyDistributedAcrossWarehouses() throws Exception {
    int scaleFactor = 10;
    int startWarehouseIndex = 1;
    int endWarehouseIndex = 3;
    int stride = 1;
    int terminals = 10;

    this.workConf.setScaleFactor(scaleFactor);
    this.workConf.setStartWarehouseIndex(startWarehouseIndex);
    this.workConf.setEndWarehouseIndex(endWarehouseIndex);
    this.workConf.setStride(stride);
    this.workConf.setTerminals(terminals);

    List<Worker<? extends BenchmarkModule>> workers = this.benchmark.makeWorkers();

    assertEquals("Should create correct number of workers", terminals, workers.size());

    // Count terminals per warehouse
    Map<Integer, Integer> terminalsPerWarehouse = new HashMap<>();
    for (Worker<? extends BenchmarkModule> worker : workers) {
      TPCCWorker tpccWorker = (TPCCWorker) worker;
      int warehouseId = tpccWorker.getTerminalWarehouseID();
      terminalsPerWarehouse.merge(warehouseId, 1, Integer::sum);
    }

    // With 10 terminals across 3 warehouses, expect distribution like 3, 3, 4
    // Each warehouse should have at least floor(10/3)=3 terminals
    // and at most ceil(10/3)=4 terminals
    int numWarehouses = 3;
    int minTerminalsPerWarehouse = 3;
    int maxTerminalsPerWarehouse = 4;

    for (Map.Entry<Integer, Integer> entry : terminalsPerWarehouse.entrySet()) {
      int warehouseId = entry.getKey();
      int count = entry.getValue();
      assertTrue(
          "Warehouse "
              + warehouseId
              + " has "
              + count
              + " terminals, expected between "
              + minTerminalsPerWarehouse
              + " and "
              + maxTerminalsPerWarehouse,
          count >= minTerminalsPerWarehouse && count <= maxTerminalsPerWarehouse);
    }

    // Verify all expected warehouses have terminals
    assertEquals(
        "All warehouses should have at least one terminal",
        numWarehouses,
        terminalsPerWarehouse.size());

    // Verify total terminals assigned equals expected
    int totalAssigned = terminalsPerWarehouse.values().stream().mapToInt(Integer::intValue).sum();
    assertEquals(
        "Total terminals assigned should equal requested terminals", terminals, totalAssigned);

    // Verify each worker has sequential ID from 0 to terminals-1
    for (int i = 0; i < workers.size(); i++) {
      assertEquals("Worker ID should be sequential", i, workers.get(i).getId());
    }
  }

  /**
   * Test distributed benchmarking scenario: 3 instances each handling different warehouse subsets.
   * Instance 1: warehouses 1, 4, 7, 10 (start=1, stride=3) Instance 2: warehouses 2, 5, 8 (start=2,
   * stride=3) Instance 3: warehouses 3, 6, 9 (start=3, stride=3)
   */
  @Test
  public void testDistributedBenchmarkingScenario() throws Exception {
    int scaleFactor = 10;
    int stride = 3;
    int terminalsPerInstance = 8;

    // Simulate instance 1: warehouses 1, 4, 7, 10
    this.workConf.setScaleFactor(scaleFactor);
    this.workConf.setStartWarehouseIndex(1);
    this.workConf.setEndWarehouseIndex(10);
    this.workConf.setStride(stride);
    this.workConf.setTerminals(terminalsPerInstance);

    List<Worker<? extends BenchmarkModule>> workers1 = this.benchmark.makeWorkers();

    Set<Integer> instance1Warehouses = new HashSet<>();
    for (Worker<? extends BenchmarkModule> worker : workers1) {
      instance1Warehouses.add(((TPCCWorker) worker).getTerminalWarehouseID());
    }

    // Instance 1 should only use warehouses 1, 4, 7, 10
    Set<Integer> expectedInstance1 = Set.of(1, 4, 7, 10);
    assertEquals(
        "Instance 1 should use warehouses 1, 4, 7, 10", expectedInstance1, instance1Warehouses);

    // Simulate instance 2: warehouses 2, 5, 8
    this.workConf.setStartWarehouseIndex(2);
    this.workConf.setEndWarehouseIndex(10);
    this.benchmark =
        this.benchmark
            .getClass()
            .getConstructor(this.workConf.getClass())
            .newInstance(this.workConf);

    List<Worker<? extends BenchmarkModule>> workers2 = this.benchmark.makeWorkers();

    Set<Integer> instance2Warehouses = new HashSet<>();
    for (Worker<? extends BenchmarkModule> worker : workers2) {
      instance2Warehouses.add(((TPCCWorker) worker).getTerminalWarehouseID());
    }

    // Instance 2 should only use warehouses 2, 5, 8
    Set<Integer> expectedInstance2 = Set.of(2, 5, 8);
    assertEquals(
        "Instance 2 should use warehouses 2, 5, 8", expectedInstance2, instance2Warehouses);

    // Simulate instance 3: warehouses 3, 6, 9
    this.workConf.setStartWarehouseIndex(3);
    this.workConf.setEndWarehouseIndex(10);
    this.benchmark =
        this.benchmark
            .getClass()
            .getConstructor(this.workConf.getClass())
            .newInstance(this.workConf);

    List<Worker<? extends BenchmarkModule>> workers3 = this.benchmark.makeWorkers();

    Set<Integer> instance3Warehouses = new HashSet<>();
    for (Worker<? extends BenchmarkModule> worker : workers3) {
      instance3Warehouses.add(((TPCCWorker) worker).getTerminalWarehouseID());
    }

    // Instance 3 should only use warehouses 3, 6, 9
    Set<Integer> expectedInstance3 = Set.of(3, 6, 9);
    assertEquals(
        "Instance 3 should use warehouses 3, 6, 9", expectedInstance3, instance3Warehouses);
  }
}
