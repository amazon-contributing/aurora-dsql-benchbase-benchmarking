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

import com.oltpbenchmark.util.StringUtil;

/** Constants used throughout the benchmark execution. */
public final class BenchmarkConstants {

  // Formatting constants
  public static final String SINGLE_LINE = StringUtil.repeat("=", 70);

  // Rate constants
  public static final String RATE_DISABLED = "disabled";
  public static final String RATE_UNLIMITED = "unlimited";

  // Default values
  public static final String DEFAULT_OUTPUT_DIRECTORY = "results";
  public static final int DEFAULT_WINDOW_SIZE = 5;
  public static final double DEFAULT_CONNECTION_RATE = 10.0;
  public static final int DEFAULT_BATCH_SIZE = 128;
  public static final int DEFAULT_MAX_RETRIES = 3;

  // Namespace defaults
  public static final String DEFAULT_YCSB_NAMESPACE = "YcsbMetrics";
  public static final String DEFAULT_TPCC_NAMESPACE = "TpccMetrics";
  public static final String DEFAULT_YCSB_TESTNAME = "YcsbDefault";
  public static final String DEFAULT_TPCC_TESTNAME = "TpccDefault";

  // Private constructor to prevent instantiation
  private BenchmarkConstants() {
    throw new AssertionError("Cannot instantiate constants class");
  }

  /**
   * Get default CloudWatch namespace for a benchmark
   *
   * @param benchmarkName Name of the benchmark
   * @return Default namespace
   */
  public static String getDefaultNamespace(String benchmarkName) {
    switch (benchmarkName) {
      case "ycsb":
        return DEFAULT_YCSB_NAMESPACE;
      case "tpcc":
      default:
        return DEFAULT_TPCC_NAMESPACE;
    }
  }

  /**
   * Get default test name for a benchmark
   *
   * @param benchmarkName Name of the benchmark
   * @return Default test name
   */
  public static String getDefaultTestName(String benchmarkName) {
    switch (benchmarkName) {
      case "ycsb":
        return DEFAULT_YCSB_TESTNAME;
      case "tpcc":
      default:
        return DEFAULT_TPCC_TESTNAME;
    }
  }
}
