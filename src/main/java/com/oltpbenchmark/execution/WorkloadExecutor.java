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

import com.oltpbenchmark.Results;
import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.MonitorInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Handles the execution of benchmark workloads. */
@Slf4j
public class WorkloadExecutor {

  /**
   * Execute the workload for all benchmarks
   *
   * @param benchmarks List of benchmark modules to execute
   * @param monitorInfo Monitoring configuration
   * @return Results of the benchmark execution
   * @throws IOException if I/O operation fails
   */
  public Results execute(List<BenchmarkModule> benchmarks, MonitorInfo monitorInfo)
      throws IOException {

    List<Worker<?>> workers = createWorkers(benchmarks);
    List<WorkloadConfiguration> workConfs = new ArrayList<>();

    for (BenchmarkModule bench : benchmarks) {
      workConfs.add(bench.getWorkloadConfiguration());
      logBenchmarkStart(bench);
    }

    Results r = ThreadBench.runRateLimitedBenchmark(workers, workConfs, monitorInfo);

    log.info(BenchmarkConstants.SINGLE_LINE);
    log.info("Rate limited reqs/s: {}", r);

    return r;
  }

  /**
   * Create workers for all benchmarks
   *
   * @param benchmarks List of benchmark modules
   * @return List of workers for all benchmarks
   * @throws IOException if I/O operation fails
   */
  private List<Worker<?>> createWorkers(List<BenchmarkModule> benchmarks) throws IOException {
    List<Worker<?>> workers = new ArrayList<>();

    for (BenchmarkModule bench : benchmarks) {
      int terminals = bench.getWorkloadConfiguration().getTerminals();
      log.info("Creating {} virtual terminals...", terminals);
      workers.addAll(bench.makeWorkers());
    }

    return workers;
  }

  /**
   * Log benchmark start information
   *
   * @param bench Benchmark module
   */
  private void logBenchmarkStart(BenchmarkModule bench) {
    int numPhases = bench.getWorkloadConfiguration().getNumberOfPhases();
    log.info(
        String.format(
            "Launching the %s Benchmark with %s Phase%s...",
            bench.getBenchmarkName().toUpperCase(), numPhases, (numPhases > 1 ? "s" : "")));
  }
}
