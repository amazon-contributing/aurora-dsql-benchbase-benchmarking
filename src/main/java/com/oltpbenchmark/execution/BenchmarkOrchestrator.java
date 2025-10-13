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
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.util.MonitorInfo;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.XMLConfiguration;

/** Orchestrates the benchmark execution workflow. */
@Slf4j
public class BenchmarkOrchestrator {

  private final CommandLineHandler commandLineHandler;
  private final BenchmarkConfigurationFactory benchmarkFactory;
  private final DatabaseOperationsManager dbOpsManager;
  private final WorkloadExecutor workloadExecutor;
  private final ResultsManager resultsManager;
  private final AnonymizationHandler anonymizationHandler;

  public BenchmarkOrchestrator() throws Exception {
    // Load plugin configuration first
    XMLConfiguration pluginConfig = ConfigurationLoader.loadXMLConfiguration("config/plugin.xml");

    // Initialize components
    this.commandLineHandler = new CommandLineHandler(pluginConfig);
    this.benchmarkFactory = new BenchmarkConfigurationFactory(new TransactionTypeLoader());
    this.dbOpsManager = new DatabaseOperationsManager();
    this.workloadExecutor = new WorkloadExecutor();
    this.resultsManager = new ResultsManager();
    this.anonymizationHandler = new AnonymizationHandler();
  }

  /**
   * Run the benchmark based on command line arguments
   *
   * @param args Command line arguments
   * @throws Exception if execution fails
   */
  public void run(String[] args) throws Exception {
    // Parse command line
    CommandLine argsLine = commandLineHandler.parse(args);

    // Handle help
    if (commandLineHandler.isHelpRequested(argsLine)) {
      commandLineHandler.printUsage();
      return;
    }

    // Validate required arguments
    if (!commandLineHandler.validateRequiredArguments(argsLine)) {
      commandLineHandler.printUsage();
      return;
    }

    // Load configurations
    String configFile = argsLine.getOptionValue("c");
    XMLConfiguration xmlConfig = ConfigurationLoader.loadXMLConfiguration(configFile);
    XMLConfiguration pluginConfig = ConfigurationLoader.loadXMLConfiguration("config/plugin.xml");

    // Build monitoring configuration
    MonitorInfo monitorInfo = benchmarkFactory.buildMonitorInfo(argsLine);

    // Get target benchmarks
    String targetBenchmarks = argsLine.getOptionValue("b");
    String[] targetList = targetBenchmarks.split(",");

    // Create benchmarks
    BenchmarkConfigurationFactory.FactoryResult factoryResult =
        benchmarkFactory.createBenchmarks(
            targetList, argsLine, xmlConfig, pluginConfig, monitorInfo);

    List<BenchmarkModule> benchmarks = factoryResult.getBenchmarks();
    List<TransactionType> activeTxTypes = factoryResult.getActiveTxTypes();

    // Handle dialect export if requested
    if (CommandLineHandler.isBooleanOptionSet(argsLine, "dialects-export")) {
      handleDialectExport(benchmarks.get(0));
      return;
    }

    // Execute database operations
    executeDatabaseOperations(argsLine, benchmarks);

    // Refresh catalogs
    dbOpsManager.refreshCatalogs(benchmarks);

    // Handle anonymization if requested
    if (CommandLineHandler.isBooleanOptionSet(argsLine, "anonymize")) {
      anonymizationHandler.applyAnonymization(xmlConfig, configFile);
    }

    // Execute workload if requested
    if (CommandLineHandler.isBooleanOptionSet(argsLine, "execute")) {
      executeWorkload(benchmarks, activeTxTypes, argsLine, xmlConfig, monitorInfo);
    } else {
      log.info("Skipping benchmark workload execution");
    }
  }

  private void executeDatabaseOperations(CommandLine argsLine, List<BenchmarkModule> benchmarks)
      throws SQLException, IOException, InterruptedException {

    // Create databases
    if (CommandLineHandler.isBooleanOptionSet(argsLine, "create")) {
      try {
        dbOpsManager.createDatabases(benchmarks);
        // Refresh catalog after creation
      } catch (Throwable ex) {
        log.error("Unexpected error when creating benchmark database tables.", ex);
        System.exit(1);
      }
    } else {
      log.debug("Skipping creating benchmark database tables");
    }
    dbOpsManager.refreshCatalogs(benchmarks);
    // Clear databases
    if (CommandLineHandler.isBooleanOptionSet(argsLine, "clear")) {
      try {
        dbOpsManager.clearDatabases(benchmarks);
      } catch (Throwable ex) {
        log.error("Unexpected error when clearing benchmark database tables.", ex);
        System.exit(1);
      }
    } else {
      log.debug("Skipping clearing benchmark database tables");
    }

    // Load data
    if (CommandLineHandler.isBooleanOptionSet(argsLine, "load")) {
      try {
        dbOpsManager.loadData(benchmarks);
      } catch (Throwable ex) {
        log.error("Unexpected error when loading benchmark database records.", ex);
        System.exit(1);
      }
    } else {
      log.debug("Skipping loading benchmark database records");
    }
  }

  private void executeWorkload(
      List<BenchmarkModule> benchmarks,
      List<TransactionType> activeTxTypes,
      CommandLine argsLine,
      XMLConfiguration xmlConfig,
      MonitorInfo monitorInfo)
      throws Exception {

    try {
      // Execute workload
      Results results = workloadExecutor.execute(benchmarks, monitorInfo);

      // Write histograms to console
      resultsManager.writeHistograms(results);

      // Build results configuration
      ResultsConfiguration resultsConfig =
          buildResultsConfiguration(argsLine, xmlConfig, activeTxTypes);

      // Write results to files
      resultsManager.writeResults(results, resultsConfig);

    } catch (Throwable ex) {
      log.error("Unexpected error when executing benchmarks.", ex);
      System.exit(1);
    }
  }

  private ResultsConfiguration buildResultsConfiguration(
      CommandLine argsLine, XMLConfiguration xmlConfig, List<TransactionType> activeTxTypes) {

    String outputDirectory =
        argsLine.hasOption("d")
            ? argsLine.getOptionValue("d")
            : BenchmarkConstants.DEFAULT_OUTPUT_DIRECTORY;

    int windowSize =
        Integer.parseInt(
            argsLine.getOptionValue("s", String.valueOf(BenchmarkConstants.DEFAULT_WINDOW_SIZE)));

    String name = String.join("-", argsLine.getOptionValue("b").split(","));
    String baseFileName = name + "_" + com.oltpbenchmark.util.TimeUtil.getCurrentTimeString();

    return new ResultsConfiguration(
        outputDirectory, baseFileName, windowSize, activeTxTypes, argsLine, xmlConfig);
  }

  private void handleDialectExport(BenchmarkModule bench) {
    if (bench.getStatementDialects() != null) {
      log.info("Exporting StatementDialects for {}", bench);
      String xml =
          bench
              .getStatementDialects()
              .export(
                  bench.getWorkloadConfiguration().getDatabaseType(),
                  bench.getProcedures().values());
      log.debug(xml);
      System.exit(0);
    }
    throw new RuntimeException("No StatementDialects is available for " + bench);
  }
}
