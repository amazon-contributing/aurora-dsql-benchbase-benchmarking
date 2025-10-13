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

package com.oltpbenchmark.execution;

import com.oltpbenchmark.api.BenchmarkModule;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Manages database operations for benchmarks including creating, clearing, and loading data. */
@Slf4j
public class DatabaseOperationsManager {
  /**
   * Create database tables for all benchmarks
   *
   * @param benchmarks List of benchmark modules
   * @throws SQLException if database operation fails
   * @throws IOException if I/O operation fails
   */
  public void createDatabases(List<BenchmarkModule> benchmarks) throws SQLException, IOException {
    for (BenchmarkModule benchmark : benchmarks) {
      log.info("Creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
      createDatabase(benchmark);
      log.info("Finished creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
    }
  }

  /**
   * Clear database tables for all benchmarks
   *
   * @param benchmarks List of benchmark modules
   * @throws SQLException if database operation fails
   */
  public void clearDatabases(List<BenchmarkModule> benchmarks) throws SQLException {
    for (BenchmarkModule benchmark : benchmarks) {
      log.info("Clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
      benchmark.refreshCatalog();
      benchmark.clearDatabase();
      benchmark.refreshCatalog();
      log.info("Finished clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
    }
  }

  /**
   * Load data into all benchmarks
   *
   * @param benchmarks List of benchmark modules
   * @throws IOException if I/O operation fails
   * @throws SQLException if database operation fails
   * @throws InterruptedException if loading is interrupted
   */
  public void loadData(List<BenchmarkModule> benchmarks)
      throws IOException, SQLException, InterruptedException {
    for (BenchmarkModule benchmark : benchmarks) {
      log.info("Loading data into {} database...", benchmark.getBenchmarkName().toUpperCase());
      loadDatabase(benchmark);
      log.info(
          "Finished loading data into {} database...", benchmark.getBenchmarkName().toUpperCase());
    }
  }

  /**
   * Refresh catalogs for all benchmarks
   *
   * @param benchmarks List of benchmark modules
   * @throws SQLException if database operation fails
   */
  public void refreshCatalogs(List<BenchmarkModule> benchmarks) throws SQLException {
    for (BenchmarkModule benchmark : benchmarks) {
      benchmark.refreshCatalog();
    }
  }

  /**
   * Create database for a single benchmark
   *
   * @param bench Benchmark module
   * @throws SQLException if database operation fails
   * @throws IOException if I/O operation fails
   */
  private void createDatabase(BenchmarkModule bench) throws SQLException, IOException {
    log.debug(String.format("Creating %s Database", bench));
    bench.createDatabase();
  }

  /**
   * Load database for a single benchmark
   *
   * @param bench Benchmark module
   * @throws IOException if I/O operation fails
   * @throws SQLException if database operation fails
   * @throws InterruptedException if loading is interrupted
   */
  private void loadDatabase(BenchmarkModule bench)
      throws IOException, SQLException, InterruptedException {
    log.debug(String.format("Loading %s Database", bench));
    bench.loadDatabase();
  }
}
