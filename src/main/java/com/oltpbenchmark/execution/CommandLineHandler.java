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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.configuration2.XMLConfiguration;

/** Handles command-line argument parsing and options building for the benchmark. */
@Slf4j
public class CommandLineHandler {
  private final Options options;

  public CommandLineHandler(XMLConfiguration pluginConfig) {
    this.options = buildOptions(pluginConfig);
  }

  /**
   * Parse command line arguments
   *
   * @param args Command line arguments
   * @return CommandLine object with parsed arguments
   * @throws ParseException if parsing fails
   */
  public CommandLine parse(String[] args) throws ParseException {
    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }

  /**
   * Check if help was requested
   *
   * @param commandLine Parsed command line
   * @return true if help was requested
   */
  public boolean isHelpRequested(CommandLine commandLine) {
    return commandLine.hasOption("h");
  }

  /**
   * Validate required arguments
   *
   * @param commandLine Parsed command line
   * @return true if all required arguments are present
   */
  public boolean validateRequiredArguments(CommandLine commandLine) {
    if (!commandLine.hasOption("c")) {
      log.error("Missing Configuration file");
      return false;
    }
    if (!commandLine.hasOption("b")) {
      log.error("Missing Benchmark Class to load");
      return false;
    }
    return true;
  }

  /** Print usage information */
  public void printUsage() {
    HelpFormatter hlpfrmt = new HelpFormatter();
    hlpfrmt.printHelp("benchbase", options);
  }

  /**
   * Check if a boolean option is set to true
   *
   * @param commandLine Parsed command line
   * @param key Option key
   * @return true if the option is set to true
   */
  public static boolean isBooleanOptionSet(CommandLine commandLine, String key) {
    if (commandLine.hasOption(key)) {
      log.debug("CommandLine has option '{}'. Checking whether set to true", key);
      String val = commandLine.getOptionValue(key);
      log.debug(String.format("CommandLine %s => %s", key, val));
      return (val != null && val.equalsIgnoreCase("true"));
    }
    return false;
  }

  private Options buildOptions(XMLConfiguration pluginConfig) {
    Options options = new Options();

    // Core options
    options.addOption(
        "b",
        "bench",
        true,
        "[required] Benchmark class. Currently supported: "
            + pluginConfig.getList("/plugin//@name"));
    options.addOption("c", "config", true, "[required] Workload configuration file");

    // Database operations
    options.addOption(null, "create", true, "Initialize the database for this benchmark");
    options.addOption(null, "clear", true, "Clear all records in the database for this benchmark");
    options.addOption(null, "load", true, "Load data using the benchmark's data loader");
    options.addOption(
        null, "anonymize", true, "Anonymize specified datasets using differential privacy");
    options.addOption(null, "execute", true, "Execute the benchmark workload");

    // Help and output options
    options.addOption("h", "help", false, "Print this help");
    options.addOption("s", "sample", true, "Sampling window");
    options.addOption("im", "interval-monitor", true, "Monitoring Interval in milliseconds");
    options.addOption("mt", "monitor-type", true, "Type of Monitoring (throughput/advanced)");
    options.addOption(
        "d",
        "directory",
        true,
        "Base directory for the result files, default is current directory");
    options.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");
    options.addOption("jh", "json-histograms", true, "Export histograms to JSON file");

    // Database configuration
    options.addOption(null, "type", true, "Type of database. For e.g. POSTGRES");
    options.addOption(null, "driver", true, "Driver class name. For e.g. org.postgresql.Driver");
    options.addOption(null, "url", true, "URL to connect with the database");
    options.addOption(null, "reconnectOnConnectionFailure", false, "Should reconnect on failure?");
    options.addOption(
        null, "isolation", true, "Isolation level. For e.g. TRANSACTION_SERIALIZABLE");
    options.addOption(null, "batchsize", true, "Batch size. For e.g. 128");
    options.addOption(null, "username", true, "Username to connect with the database.");
    options.addOption(
        null,
        "scalefactor",
        true,
        "For TPCC, Number of warehouses. For e.g. 100. For YCSB, Scalefactor is *1000 the number of rows in the USERTABLE");

    // Monitoring and performance options
    options.addOption(
        null,
        "benchmarkTestName",
        true,
        "Benchmark test name. CW metrics are published under this name.");
    options.addOption(null, "publishToCloudWatch", false, "Publish to CloudWatch.");
    options.addOption(null, "region", true, "AWS region.");
    options.addOption(null, "retries", true, "Number of retries.");
    options.addOption(null, "loaderThreads", true, "Number of loader threads.");

    // TPCC specific options
    options.addOption(null, "skipIndexBuild", false, "Skip index build.");
    options.addOption(null, "skipItemLoad", false, "Skip item load.");
    options.addOption(null, "skipMainDataLoad", false, "Skip main data load.");
    options.addOption(null, "terminals", true, "Number of terminals.");
    options.addOption(null, "time", true, "Number of seconds to run the execution phase");
    options.addOption(null, "stride", true, "Stride to cover warehouses. For e.g. 1");
    options.addOption(
        null,
        "startWarehouseIndex",
        true,
        "Start warehouse index for TPCC (1 based index). For e.g. 1");
    options.addOption(
        null,
        "endWarehouseIndex",
        true,
        "End warehouse index for TPCC (1 based index). For e.g. 100");

    return options;
  }
}
