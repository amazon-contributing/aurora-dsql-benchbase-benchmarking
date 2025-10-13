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

package com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql;

import com.google.common.base.Preconditions;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders.CustomerTableLoader;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders.DistrictTableLoader;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders.HistoryTableLoader;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders.ItemTableLoader;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders.NewOrderTableLoader;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders.OrderLineTableLoader;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders.OrderTableLoader;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders.StockTableLoader;
import com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql.loaders.WarehouseTableLoader;
import com.oltpbenchmark.types.DatabaseType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Refactored TPC-C Benchmark Loader for Aurora DSQL
 *
 * <p>This is a cleaner, more maintainable version of the original DSQLTPCCLoader. Key improvements:
 * - Separated concerns with dedicated manager classes - Centralized constants - Cleaner retry logic
 * - Modular table loaders - Better error handling
 */
public final class DSQLTPCCLoader extends Loader<TPCCBenchmark> {

  private final int startWarehouseIndex;
  private final int endWarehouseIndex;
  private final int stride;

  private final ConnectionManager connectionManager;
  private final RetryHandler retryHandler;

  public DSQLTPCCLoader(TPCCBenchmark benchmark) {
    super(benchmark);

    validateConfiguration();

    this.startWarehouseIndex = workConf.getStartWarehouseIndex();
    this.endWarehouseIndex = workConf.getEndWarehouseIndex();
    this.stride = workConf.getStride();

    this.connectionManager = new ConnectionManager(benchmark);
    this.retryHandler = new RetryHandler(workConf.getMaxRetries(), connectionManager);

    LOG.info(
        "TPCC Loader Configuration: skipMainData={}, skipItems={}, skipIndex={}, "
            + "warehouses=[{}-{}], stride={}",
        workConf.skipMainDataLoad(),
        workConf.skipItemLoad(),
        workConf.skipIndexBuild(),
        startWarehouseIndex,
        endWarehouseIndex,
        stride);
  }

  private void validateConfiguration() {
    Preconditions.checkArgument(
        workConf.getStartWarehouseIndex() >= 1,
        "Start warehouse index must be >= 1, but was: %s",
        (Object) workConf.getStartWarehouseIndex());

    Preconditions.checkArgument(
        workConf.getEndWarehouseIndex() >= 1,
        "End warehouse index must be >= 1, but was: %s",
        (Object) workConf.getEndWarehouseIndex());

    Preconditions.checkArgument(
        workConf.getEndWarehouseIndex() <= workConf.getScaleFactor(),
        "End warehouse index must be <= scale factor. End index: %s, Scale factor: %s",
        (Object) workConf.getEndWarehouseIndex(),
        (Object) workConf.getScaleFactor());

    Preconditions.checkArgument(
        workConf.getStride() >= 1,
        "Stride must be >= 1, but was: %s",
        (Object) workConf.getStride());

    Preconditions.checkArgument(
        workConf.getStartWarehouseIndex() <= workConf.getEndWarehouseIndex(),
        "Start warehouse index must be <= end warehouse index. Start: %s, End: %s",
        (Object) workConf.getStartWarehouseIndex(),
        (Object) workConf.getEndWarehouseIndex());
  }

  @Override
  public List<LoaderThread> createLoaderThreads() {
    List<LoaderThread> threads = new ArrayList<>();

    // Calculate number of warehouses to load
    int numWarehouses = calculateWarehouseCount();

    // Create latches for coordination
    CountDownLatch itemLatch = new CountDownLatch(1);
    CountDownLatch indexLatch = new CountDownLatch(1);
    CountDownLatch allThreadLatch = new CountDownLatch(2 + numWarehouses);

    // Create threads
    threads.add(createIndexThread(indexLatch, allThreadLatch));
    threads.add(createItemLoaderThread(itemLatch, indexLatch, allThreadLatch));
    threads.addAll(createWarehouseLoaderThreads(itemLatch, indexLatch, allThreadLatch));

    return threads;
  }

  private int calculateWarehouseCount() {
    int count = 0;
    for (int w = startWarehouseIndex; w <= endWarehouseIndex; w += stride) {
      count++;
    }
    return count;
  }

  private LoaderThread createIndexThread(CountDownLatch indexLatch, CountDownLatch allThreadLatch) {
    if (workConf.skipIndexBuild() || !DatabaseType.AURORADSQL.equals(workConf.getDatabaseType())) {
      LOG.info("Skipping index creation");
      indexLatch.countDown();
      allThreadLatch.countDown();
      return new NoOpLoaderThread(this.benchmark);
    }
    return new LoaderThread(this.benchmark) {
      @Override
      public void load(Connection conn) {
        try {
          createIndexAsync(conn);
        } catch (SQLException e) {
          throw new RuntimeException("Failed to create index", e);
        }
      }

      @Override
      public void afterLoad() {
        indexLatch.countDown();
        allThreadLatch.countDown();
      }
    };
  }

  private LoaderThread createItemLoaderThread(
      CountDownLatch itemLatch, CountDownLatch indexLatch, CountDownLatch allThreadLatch) {
    if (workConf.skipItemLoad()) {
      LOG.info("Skipping item load");
      itemLatch.countDown();
      allThreadLatch.countDown();
      return new NoOpLoaderThread(this.benchmark);
    }

    return new LoaderThread(this.benchmark) {
      @Override
      public void load(Connection conn) {
        String threadName = TPCCLoaderConstants.LOAD_ITEMS_THREAD_NAME;
        try {
          connectionManager.createConnection(threadName);

          ItemTableLoader itemLoader =
              new ItemTableLoader(
                  benchmark, connectionManager, retryHandler, workConf.getBatchSize());

          itemLoader.load(threadName);

        } catch (SQLException e) {
          throw new RuntimeException("Failed to load items", e);
        }
      }

      @Override
      public void beforeLoad() {
        try {
          indexLatch.await();
          Thread.sleep(TPCCLoaderConstants.POST_INDEX_WAIT_MS);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting for index creation", e);
        }
      }

      @Override
      public void afterLoad() {
        itemLatch.countDown();
        allThreadLatch.countDown();
        connectionManager.closeResourcesForThread(TPCCLoaderConstants.LOAD_ITEMS_THREAD_NAME);
      }
    };
  }

  private List<LoaderThread> createWarehouseLoaderThreads(
      CountDownLatch itemLatch, CountDownLatch indexLatch, CountDownLatch allThreadLatch) {
    List<LoaderThread> threads = new ArrayList<>();

    if (workConf.skipMainDataLoad()) {
      LOG.info("Skipping main data load");
      int numWarehouses = calculateWarehouseCount();
      for (int i = 0; i < numWarehouses; i++) {
        allThreadLatch.countDown();
      }
      return threads;
    }

    for (int w = startWarehouseIndex; w <= endWarehouseIndex; w += stride) {
      final int warehouseId = w;
      threads.add(createWarehouseThread(warehouseId, itemLatch, indexLatch, allThreadLatch));
    }

    return threads;
  }

  private LoaderThread createWarehouseThread(
      int warehouseId,
      CountDownLatch itemLatch,
      CountDownLatch indexLatch,
      CountDownLatch allThreadLatch) {
    return new LoaderThread(this.benchmark) {
      private final String threadName = String.valueOf(warehouseId);

      @Override
      public void beforeLoad() {
        try {
          indexLatch.await();
          itemLatch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting", e);
        }
      }

      @Override
      public void load(Connection conn) {
        try {
          connectionManager.createConnection(threadName);
          loadWarehouseData(warehouseId);
        } catch (SQLException e) {
          throw new RuntimeException("Failed to load warehouse " + warehouseId, e);
        }
      }

      @Override
      public void afterLoad() {
        allThreadLatch.countDown();
        connectionManager.closeResourcesForThread(threadName);
      }

      private void loadWarehouseData(int warehouseId) throws SQLException {

        LOG.info("Loading all data for warehouse {}", warehouseId);

        // Create loader instances
        WarehouseTableLoader warehouseLoader =
            new WarehouseTableLoader(
                benchmark, connectionManager, retryHandler, workConf.getBatchSize());
        StockTableLoader stockLoader =
            new StockTableLoader(
                benchmark, connectionManager, retryHandler, workConf.getBatchSize());
        DistrictTableLoader districtLoader =
            new DistrictTableLoader(
                benchmark, connectionManager, retryHandler, workConf.getBatchSize());
        CustomerTableLoader customerLoader =
            new CustomerTableLoader(
                benchmark, connectionManager, retryHandler, workConf.getBatchSize());
        HistoryTableLoader historyLoader =
            new HistoryTableLoader(
                benchmark, connectionManager, retryHandler, workConf.getBatchSize());
        OrderTableLoader orderLoader =
            new OrderTableLoader(
                benchmark, connectionManager, retryHandler, workConf.getBatchSize());
        NewOrderTableLoader newOrderLoader =
            new NewOrderTableLoader(
                benchmark, connectionManager, retryHandler, workConf.getBatchSize());
        OrderLineTableLoader orderLineLoader =
            new OrderLineTableLoader(
                benchmark, connectionManager, retryHandler, workConf.getBatchSize());

        try {
          // Load in dependency order
          warehouseLoader.load(threadName, warehouseId);
          stockLoader.load(threadName, warehouseId);
          districtLoader.load(threadName, warehouseId);
          customerLoader.load(threadName, warehouseId);
          historyLoader.load(threadName, warehouseId);
          orderLoader.load(threadName, warehouseId);
          newOrderLoader.load(threadName, warehouseId);
          orderLineLoader.load(threadName, warehouseId);
        } catch (SQLException e) {
          throw new RuntimeException("Failed to load data for warehouse " + warehouseId, e);
        }
      }
    };
  }

  private void createIndexAsync(Connection conn) throws SQLException {
    String jobId = submitIndexCreation(conn);
    waitForIndexCompletion(conn, jobId);
  }

  private String submitIndexCreation(Connection conn) throws SQLException {
    try (PreparedStatement stmt =
            conn.prepareStatement(TPCCLoaderConstants.CREATE_CUSTOMER_INDEX_ASYNC);
        ResultSet rs = stmt.executeQuery()) {
      if (rs.next()) {
        String jobId = rs.getString(1);
        LOG.info("Index creation job started with job_id: {}", jobId);
        return jobId;
      } else {
        throw new SQLException("Index creation request didn't return job id");
      }
    }
  }

  private void waitForIndexCompletion(Connection conn, String jobId) throws SQLException {
    String status = TPCCLoaderConstants.JOB_STATUS_PROCESSING;

    try (PreparedStatement stmt = conn.prepareStatement(TPCCLoaderConstants.SELECT_JOB_STATUS)) {
      stmt.setString(1, jobId);

      while (!TPCCLoaderConstants.JOB_STATUS_COMPLETED.equalsIgnoreCase(status)) {
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            status = rs.getString("status");
            LOG.info("Index creation job {} status: {}", jobId, status);
          }

          if (!TPCCLoaderConstants.JOB_STATUS_COMPLETED.equalsIgnoreCase(status)) {
            Thread.sleep(TPCCLoaderConstants.INDEX_CHECK_INTERVAL_MS);
          }
        } catch (InterruptedException e) {
          throw new SQLException("Interrupted while waiting for index creation", e);
        }
      }
    }
  }

  /** No-op loader thread for skipped operations */
  private static class NoOpLoaderThread extends LoaderThread {
    public NoOpLoaderThread(TPCCBenchmark benchmark) {
      super(benchmark);
    }

    @Override
    public void load(Connection conn) {
      // No-op
    }
  }
}
