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

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.types.DatabaseType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;

/** Handles loading and parsing of configuration files. */
@Slf4j
public class ConfigurationLoader {
  /**
   * Build an XMLConfiguration from a file
   *
   * @param filename Path to the configuration file
   * @return XMLConfiguration object
   * @throws ConfigurationException if the file cannot be loaded or parsed
   */
  public static XMLConfiguration loadXMLConfiguration(String filename)
      throws ConfigurationException {
    Parameters params = new Parameters();
    FileBasedConfigurationBuilder<XMLConfiguration> builder =
        new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
            .configure(
                params
                    .xml()
                    .setFileName(filename)
                    .setListDelimiterHandler(new DisabledListDelimiterHandler())
                    .setExpressionEngine(new XPathExpressionEngine()));
    return builder.getConfiguration();
  }

  /**
   * Load workload configuration for a specific plugin
   *
   * @param plugin The plugin/benchmark name
   * @param argsLine Command line arguments
   * @param xmlConfig XML configuration
   * @return Configured WorkloadConfiguration object
   */
  public static WorkloadConfiguration loadWorkloadConfiguration(
      String plugin, CommandLine argsLine, XMLConfiguration xmlConfig) {

    WorkloadConfiguration wrkld = new WorkloadConfiguration();
    wrkld.setBenchmarkName(plugin);
    wrkld.setXmlConfig(xmlConfig);

    OptionsRetriever optionsRetriever = new OptionsRetriever(argsLine, xmlConfig);

    configureDatabaseSettings(wrkld, optionsRetriever);
    configureBenchmarkSettings(wrkld, optionsRetriever);
    configureBenchmarkLoaderSettings(wrkld, optionsRetriever);
    configureMetricSettings(wrkld, optionsRetriever);
    configureTerminals(wrkld, argsLine, xmlConfig, plugin);
    configureOtherSettings(wrkld, optionsRetriever, xmlConfig);

    return wrkld;
  }

  private static void configureDatabaseSettings(
      WorkloadConfiguration wrkld, OptionsRetriever optionsRetriever) {
    wrkld.setDatabaseType(DatabaseType.get(optionsRetriever.getString("type")));
    wrkld.setDriverClass(optionsRetriever.getString("driver"));
    wrkld.setUrl(optionsRetriever.getString("url"));
    wrkld.setUsername(optionsRetriever.getString("username"));
    wrkld.setPassword(optionsRetriever.getString("password"));
    wrkld.setNewConnectionPerTxn(optionsRetriever.getBoolean("newConnectionPerTxn", false));
    wrkld.setReconnectOnConnectionFailure(
        optionsRetriever.getBoolean("reconnectOnConnectionFailure", false));
  }

  private static void configureBenchmarkSettings(
      WorkloadConfiguration wrkld, OptionsRetriever optionsRetriever) {
    wrkld.setConnectionRate(
        optionsRetriever.getDouble("connectionRate", BenchmarkConstants.DEFAULT_CONNECTION_RATE));
    wrkld.setRandomSeed(optionsRetriever.getInt("randomSeed", -1));
    wrkld.setBatchSize(optionsRetriever.getInt("batchsize", BenchmarkConstants.DEFAULT_BATCH_SIZE));
    wrkld.setMaxRetries(optionsRetriever.getInt("retries", BenchmarkConstants.DEFAULT_MAX_RETRIES));
    wrkld.setDataDir(optionsRetriever.getString("datadir", "."));
    wrkld.setDDLPath(optionsRetriever.getString("ddlpath", null));

    wrkld.setScaleFactor(optionsRetriever.getDouble("scalefactor", 1.0));

    wrkld.setStride(optionsRetriever.getInt("stride", 1));
    wrkld.setStartWarehouseIndex(optionsRetriever.getInt("startWarehouseIndex", 1));
    wrkld.setEndWarehouseIndex(
        optionsRetriever.getInt("endWarehouseIndex", (int) wrkld.getScaleFactor()));

    wrkld.setRunTimeInSeconds(optionsRetriever.getInt("time", UNINITIALIZED_TIME));
  }

  private static void configureBenchmarkLoaderSettings(
      WorkloadConfiguration wrkld, OptionsRetriever optionsRetriever) {
    wrkld.setLoaderThreads(optionsRetriever.getInt("loaderThreads", wrkld.getLoaderThreads()));

    wrkld.setSkipIndexBuild(optionsRetriever.getBooleanWithoutArg("skipIndexBuild", true));
    wrkld.setSkipItemLoad(optionsRetriever.getBooleanWithoutArg("skipItemLoad", false));
    wrkld.setSkipMainDataLoad(optionsRetriever.getBooleanWithoutArg("skipMainDataLoad", false));
  }

  private static void configureMetricSettings(
      WorkloadConfiguration wrkld, OptionsRetriever optionsRetriever) {
    wrkld.setDisableLocalMetrics(optionsRetriever.getBoolean("disableLocalMetrics", false));

    wrkld.setPublishToCloudWatch(optionsRetriever.getBoolean("publishToCloudWatch", false));
    wrkld.setBenchmarkTestName(
        optionsRetriever.getString(
            "benchmarkTestName", BenchmarkConstants.getDefaultTestName(wrkld.getBenchmarkName())));
    wrkld.setNamespace(
        optionsRetriever.getString(
            "namespace", BenchmarkConstants.getDefaultNamespace(wrkld.getBenchmarkName())));
    wrkld.setRegion(optionsRetriever.getString("region", null));
  }

  private static void configureTerminals(
      WorkloadConfiguration wrkld,
      CommandLine argsLine,
      XMLConfiguration xmlConfig,
      String plugin) {
    String pluginXpathString = "[@bench='" + plugin + "']";
    int terminals;

    if (argsLine.hasOption("terminals")) {
      terminals = Integer.parseInt(argsLine.getOptionValue("terminals"));
    } else {
      terminals = xmlConfig.getInt("terminals[not(@bench)]", 0);
      terminals = xmlConfig.getInt("terminals" + pluginXpathString, terminals);
    }
    wrkld.setTerminals(terminals);
  }

  private static void configureOtherSettings(
      WorkloadConfiguration wrkld, OptionsRetriever optionsRetriever, XMLConfiguration xmlConfig) {
    String pluginXpathString = "[@bench='" + wrkld.getBenchmarkName() + "']";
    String isolationMode =
        xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
    wrkld.setIsolationMode(
        optionsRetriever.getString("isolation" + pluginXpathString, isolationMode));

    // Set selectivity if available
    try {
      double selectivity = xmlConfig.getDouble("selectivity");
      wrkld.setSelectivity(selectivity);
    } catch (Exception e) {
      // Selectivity is optional, so we ignore if not present
    }
  }
}
