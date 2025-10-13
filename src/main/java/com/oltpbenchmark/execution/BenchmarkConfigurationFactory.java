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

import static com.oltpbenchmark.WorkloadConfiguration.UNINITIALIZED_TIME;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.ImmutableMonitorInfo;
import com.oltpbenchmark.util.MonitorInfo;
import com.oltpbenchmark.util.StringUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

/** Factory for creating and configuring benchmark modules. */
@Slf4j
@RequiredArgsConstructor
public class BenchmarkConfigurationFactory {
  private final TransactionTypeLoader txnTypeLoader;

  /**
   * Create benchmark modules from configuration
   *
   * @param targetList Array of benchmark names to create
   * @param argsLine Command line arguments
   * @param xmlConfig XML configuration
   * @param pluginConfig Plugin configuration
   * @param monitorInfo Monitoring configuration
   * @return Result containing benchmark modules and active transaction types
   * @throws Exception if creation fails
   */
  public FactoryResult createBenchmarks(
      String[] targetList,
      CommandLine argsLine,
      XMLConfiguration xmlConfig,
      XMLConfiguration pluginConfig,
      MonitorInfo monitorInfo)
      throws Exception {

    List<BenchmarkModule> benchList = new ArrayList<>();
    List<TransactionType> activeTXTypes = new ArrayList<>();
    int lastTxnId = 0;

    for (String plugin : targetList) {

      String pluginXpathString = "[@bench='" + plugin + "']";
      int numTxnTypes =
          xmlConfig
              .configurationsAt("transactiontypes" + pluginXpathString + "/transactiontype")
              .size();
      if (numTxnTypes == 0 && targetList.length == 1) {
        // if it is a single workload run, <transactiontypes /> w/o attribute is used
        pluginXpathString = "[not(@bench)]";
        numTxnTypes =
            xmlConfig
                .configurationsAt("transactiontypes" + pluginXpathString + "/transactiontype")
                .size();
      }

      BenchmarkCreationResult result =
          createBenchmark(
              plugin, pluginXpathString, argsLine, xmlConfig, pluginConfig, monitorInfo, lastTxnId);

      benchList.add(result.benchmark);
      activeTXTypes.addAll(result.activeTxTypes);
      lastTxnId = result.lastTxnId;
    }

    return new FactoryResult(benchList, activeTXTypes);
  }

  /**
   * Build monitoring configuration from command line
   *
   * @param argsLine Command line arguments
   * @return MonitorInfo configuration
   * @throws ParseException if parsing fails
   */
  public MonitorInfo buildMonitorInfo(CommandLine argsLine) throws ParseException {
    ImmutableMonitorInfo.Builder builder = ImmutableMonitorInfo.builder();

    if (argsLine.hasOption("im")) {
      builder.monitoringInterval(Integer.parseInt(argsLine.getOptionValue("im")));
    }

    if (argsLine.hasOption("mt")) {
      switch (argsLine.getOptionValue("mt")) {
        case "advanced":
          builder.monitoringType(MonitorInfo.MonitoringType.ADVANCED);
          break;
        case "throughput":
          builder.monitoringType(MonitorInfo.MonitoringType.THROUGHPUT);
          break;
        default:
          throw new ParseException(
              "Monitoring type '"
                  + argsLine.getOptionValue("mt")
                  + "' is undefined, allowed values are: advanced/throughput");
      }
    }

    return builder.build();
  }

  private BenchmarkCreationResult createBenchmark(
      String plugin,
      String pluginXpathString,
      CommandLine argsLine,
      XMLConfiguration xmlConfig,
      XMLConfiguration pluginConfig,
      MonitorInfo monitorInfo,
      int lastTxnId)
      throws Exception {

    // Load workload configuration
    WorkloadConfiguration wrkld =
        ConfigurationLoader.loadWorkloadConfiguration(plugin, argsLine, xmlConfig);

    // Set monitoring if enabled
    setMonitoring(wrkld, monitorInfo, xmlConfig);

    // Create benchmark module
    BenchmarkModule bench = createBenchmarkModule(plugin, pluginConfig, wrkld);

    // Log initialization
    logBenchmarkInit(plugin, bench, wrkld);

    // Load transaction types
    TransactionTypeLoader.LoaderResult txnResult =
        txnTypeLoader.loadTransactionTypes(xmlConfig, pluginXpathString, bench, lastTxnId);
    wrkld.setTransTypes(txnResult.getTransactionTypes());

    // Load transaction groupings
    txnTypeLoader.loadTransactionGroupings(
        xmlConfig,
        pluginXpathString,
        txnResult.getTransactionTypes().size() - 1); // -1 for INVALID type

    // Set after load script if present
    if (xmlConfig.containsKey("afterload")) {
      bench.setAfterLoadScriptPath(xmlConfig.getString("afterload"));
    }

    // Load phases
    loadPhases(wrkld, xmlConfig, plugin, argsLine);

    // Validate phases
    validatePhases(wrkld, txnResult.getTransactionTypes().size() - 1);

    // Initialize workload
    wrkld.init();

    return new BenchmarkCreationResult(
        bench, txnResult.getActiveTxTypes(), txnResult.getLastTxnId());
  }

  private void setMonitoring(
      WorkloadConfiguration wrkld, MonitorInfo monitorInfo, XMLConfiguration xmlConfig) {
    if (monitorInfo.getMonitoringInterval() > 0
        && monitorInfo.getMonitoringType() == MonitorInfo.MonitoringType.ADVANCED
        && DatabaseType.get(xmlConfig.getString("type")).shouldCreateMonitoringPrefix()) {
      log.info("Advanced monitoring enabled, prefix will be added to queries.");
      wrkld.setAdvancedMonitoringEnabled(true);
    }
  }

  private BenchmarkModule createBenchmarkModule(
      String plugin, XMLConfiguration pluginConfig, WorkloadConfiguration wrkld) throws Exception {

    String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

    if (classname == null) {
      throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
    }

    return ClassUtil.newInstance(
        classname, new Object[] {wrkld}, new Class<?>[] {WorkloadConfiguration.class});
  }

  private void logBenchmarkInit(String plugin, BenchmarkModule bench, WorkloadConfiguration wrkld) {
    Map<String, Object> initDebug = new ListOrderedMap<>();
    initDebug.put(
        "Benchmark", String.format("%s {%s}", plugin.toUpperCase(), bench.getClass().getName()));
    initDebug.put("Type", wrkld.getDatabaseType());
    initDebug.put("Driver", wrkld.getDriverClass());
    initDebug.put("URL", wrkld.getUrl());
    initDebug.put("Isolation", wrkld.getIsolationString());
    initDebug.put("Batch Size", wrkld.getBatchSize());
    initDebug.put("Scale Factor", wrkld.getScaleFactor());
    initDebug.put("Terminals", wrkld.getTerminals());
    initDebug.put("New Connection Per Txn", wrkld.isNewConnectionPerTxn());
    initDebug.put("Reconnect on Connection Failure", wrkld.isReconnectOnConnectionFailure());

    if (wrkld.getSelectivity() != -1) {
      initDebug.put("Selectivity", wrkld.getSelectivity());
    }

    log.info("{}\n\n{}", BenchmarkConstants.SINGLE_LINE, StringUtil.formatMaps(initDebug));
    log.info(BenchmarkConstants.SINGLE_LINE);
  }

  private void loadPhases(
      WorkloadConfiguration wrkld,
      XMLConfiguration xmlConfig,
      String plugin,
      CommandLine argsLine) {
    String pluginXpathString = "[@bench='" + plugin + "']";
    String[] targetList = argsLine.getOptionValue("b").split(",");
    int terminals = wrkld.getTerminals();

    int size = xmlConfig.configurationsAt("/works/work").size();
    for (int i = 1; i < size + 1; i++) {
      final HierarchicalConfiguration<ImmutableNode> work =
          xmlConfig.configurationAt("works/work[" + i + "]");

      PhaseConfiguration phaseConfig =
          loadPhaseConfiguration(work, pluginXpathString, targetList, terminals);

      if (wrkld.getRunTimeInSeconds() != UNINITIALIZED_TIME) {
        phaseConfig.time = wrkld.getRunTimeInSeconds();
      }

      wrkld.addPhase(
          i,
          phaseConfig.time,
          phaseConfig.warmup,
          phaseConfig.rate,
          phaseConfig.weights,
          phaseConfig.rateLimited,
          phaseConfig.disabled,
          phaseConfig.serial,
          phaseConfig.timed,
          phaseConfig.activeTerminals,
          phaseConfig.arrival);
    }
  }

  private PhaseConfiguration loadPhaseConfiguration(
      HierarchicalConfiguration<ImmutableNode> work,
      String pluginTest,
      String[] targetList,
      int terminals) {

    PhaseConfiguration config = new PhaseConfiguration();

    // Load weights
    List<String> weight_strings;
    if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
      weight_strings = Arrays.asList(work.getString("weights" + pluginTest).split("\\s*,\\s*"));
    } else {
      weight_strings = Arrays.asList(work.getString("weights[not(@bench)]").split("\\s*,\\s*"));
    }

    // Parse weights
    config.weights = new ArrayList<>();
    double totalWeight = 0;
    for (String weightString : weight_strings) {
      double weight = Double.parseDouble(weightString);
      totalWeight += weight;
      config.weights.add(weight);
    }

    long roundedWeight = Math.round(totalWeight);
    if (roundedWeight != 100) {
      log.warn(
          "rounded weight [{}] does not equal 100. Original weight is [{}]",
          roundedWeight,
          totalWeight);
    }

    // Load rate configuration
    loadRateConfiguration(work, pluginTest, config);

    // Load other settings
    config.arrival =
        work.getString("@arrival", "regular").equalsIgnoreCase("POISSON")
            ? Phase.Arrival.POISSON
            : Phase.Arrival.REGULAR;
    config.serial = Boolean.parseBoolean(work.getString("serial", Boolean.FALSE.toString()));

    config.activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
    config.activeTerminals = work.getInt("active_terminals" + pluginTest, config.activeTerminals);

    if (config.serial && config.activeTerminals != 1) {
      log.warn("Serial ordering is enabled, so # of active terminals is clamped to 1.");
      config.activeTerminals = 1;
    }

    if (config.activeTerminals > terminals) {
      log.error(
          "Configuration error: Number of active terminals is bigger than "
              + "the total number of terminals");
      System.exit(-1);
    }

    config.time = work.getInt("/time", 0);

    config.warmup = work.getInt("/warmup", 0);
    config.timed = (config.time > 0);

    validatePhaseSettings(config);

    return config;
  }

  private void loadRateConfiguration(
      HierarchicalConfiguration<ImmutableNode> work, String pluginTest, PhaseConfiguration config) {

    String rateString = work.getString("rate[not(@bench)]", "");
    rateString = work.getString("rate" + pluginTest, rateString);

    parseRate(rateString, config);
  }

  private void parseRate(String rateString, PhaseConfiguration config) {
    if (rateString.equals(BenchmarkConstants.RATE_DISABLED)) {
      config.disabled = true;
      config.rate = 1;
      config.rateLimited = true;
    } else if (rateString.equals(BenchmarkConstants.RATE_UNLIMITED)) {
      config.rateLimited = false;
      config.rate = 1;
    } else if (rateString.isEmpty()) {
      log.error("Please specify the rate for phase");
      System.exit(-1);
    } else {
      try {
        config.rate = Double.parseDouble(rateString);
        config.rateLimited = true;
        if (config.rate <= 0) {
          log.error("Rate limit must be at least 0. Use unlimited or disabled values instead.");
          System.exit(-1);
        }
      } catch (NumberFormatException e) {
        log.error(
            String.format(
                "Rate string must be '%s', '%s' or a number",
                BenchmarkConstants.RATE_DISABLED, BenchmarkConstants.RATE_UNLIMITED));
        System.exit(-1);
      }
    }
  }

  private void validatePhaseSettings(PhaseConfiguration config) {
    if (!config.timed) {
      if (config.serial) {
        log.info("Timer disabled for serial run; will execute all queries exactly once.");
      } else {
        log.error(
            "Must provide positive time bound for non-serial executions. "
                + "Either provide a valid time or enable serial mode.");
        System.exit(-1);
      }
    } else if (config.serial) {
      log.info(
          "Timer enabled for serial run; will run queries serially "
              + "in a loop until the timer expires.");
    }

    if (config.warmup < 0) {
      log.error("Must provide non-negative time bound for warmup.");
      System.exit(-1);
    }
  }

  private void validatePhases(WorkloadConfiguration wrkld, int numTxnTypes) {
    int j = 0;
    for (Phase p : wrkld.getPhases()) {
      j++;
      if (p.getWeightCount() != numTxnTypes) {
        log.error(
            String.format(
                "Configuration files is inconsistent, phase %d contains %d weights "
                    + "but you defined %d transaction types",
                j, p.getWeightCount(), numTxnTypes));
        if (p.isSerial()) {
          log.error(
              "However, note that since this a serial phase, the weights are "
                  + "irrelevant (but still must be included---sorry).");
        }
        System.exit(-1);
      }
    }
  }

  /** Result of benchmark creation */
  private static class BenchmarkCreationResult {
    final BenchmarkModule benchmark;
    final List<TransactionType> activeTxTypes;
    final int lastTxnId;

    BenchmarkCreationResult(
        BenchmarkModule benchmark, List<TransactionType> activeTxTypes, int lastTxnId) {
      this.benchmark = benchmark;
      this.activeTxTypes = activeTxTypes;
      this.lastTxnId = lastTxnId;
    }
  }

  /** Configuration for a phase */
  private static class PhaseConfiguration {
    double rate;
    boolean rateLimited;
    boolean disabled;
    ArrayList<Double> weights;
    Phase.Arrival arrival;
    boolean serial;
    int activeTerminals;
    int time;
    int warmup;
    boolean timed;
  }

  /** Result of factory operations */
  public static class FactoryResult {
    private final List<BenchmarkModule> benchmarks;
    private final List<TransactionType> activeTxTypes;

    public FactoryResult(List<BenchmarkModule> benchmarks, List<TransactionType> activeTxTypes) {
      this.benchmarks = benchmarks;
      this.activeTxTypes = activeTxTypes;
    }

    public List<BenchmarkModule> getBenchmarks() {
      return benchmarks;
    }

    public List<TransactionType> getActiveTxTypes() {
      return activeTxTypes;
    }
  }
}
