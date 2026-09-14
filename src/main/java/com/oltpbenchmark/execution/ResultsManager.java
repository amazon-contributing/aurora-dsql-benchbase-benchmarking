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
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.JSONSerializable;
import com.oltpbenchmark.util.JSONUtil;
import com.oltpbenchmark.util.ResultWriter;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.TimeUtil;
import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/** Manages writing benchmark results to various output formats. */
@Slf4j
public class ResultsManager {

  /**
   * Write all results to files
   *
   * @param results Benchmark results
   * @param config Results configuration
   * @throws Exception if writing fails
   */
  public void writeResults(Results results, ResultsConfiguration config) throws Exception {

    // Ensure output directory exists
    FileUtil.makeDirIfNotExists(config.getOutputDirectory());

    // Create result writer
    ResultWriter rw = new ResultWriter(results, config.getXmlConfig(), config.getCommandLine());

    // Generate base filename
    String name =
        StringUtils.join(StringUtils.split(config.getCommandLine().getOptionValue("b"), ','), '-');
    String baseFileName = name + "_" + TimeUtil.getCurrentTimeString();

    // Write raw results
    writeRawResults(rw, config, baseFileName);

    // Write samples
    writeSamples(rw, config, baseFileName);

    // Write summary
    writeSummary(rw, config, baseFileName);

    // Write parameters
    writeParams(rw, config, baseFileName);

    // Write metrics if available
    if (rw.hasMetrics()) {
      writeMetrics(rw, config, baseFileName);
    }

    // Write configuration
    writeConfig(rw, config, baseFileName);

    // Write results CSV
    writeResultsCsv(rw, config, baseFileName);

    // Write per-transaction results
    writePerTransactionResults(rw, config, baseFileName);

    // Handle JSON histograms if requested
    if (config.shouldWriteJsonHistograms()) {
      String histogramJson = writeJSONHistograms(results);
      FileUtil.writeStringToFile(new File(config.getJsonHistogramsFileName()), histogramJson);
      log.info("Histograms JSON Data: " + config.getJsonHistogramsFileName());
    }

    // Check for errors
    if (results.getState() == State.ERROR) {
      throw new RuntimeException(
          "Errors encountered during benchmark execution. See output above for details.");
    }
  }

  /**
   * Write histograms to console
   *
   * @param results Benchmark results
   */
  public void writeHistograms(Results results) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");

    sb.append(StringUtil.bold("Completed Transactions:"))
        .append("\n")
        .append(results.getSuccess())
        .append("\n\n");

    sb.append(StringUtil.bold("Aborted Transactions:"))
        .append("\n")
        .append(results.getAbort())
        .append("\n\n");

    sb.append(StringUtil.bold("Rejected Transactions (Server Retry):"))
        .append("\n")
        .append(results.getRetry())
        .append("\n\n");

    sb.append(StringUtil.bold("Rejected Transactions (Retry Different):"))
        .append("\n")
        .append(results.getRetryDifferent())
        .append("\n\n");

    sb.append(StringUtil.bold("Unexpected SQL Errors:"))
        .append("\n")
        .append(results.getError())
        .append("\n\n");

    sb.append(StringUtil.bold("Unknown Status Transactions:"))
        .append("\n")
        .append(results.getUnknown())
        .append("\n\n");

    if (!results.getAbortMessages().isEmpty()) {
      sb.append("\n\n")
          .append(StringUtil.bold("User Aborts:"))
          .append("\n")
          .append(results.getAbortMessages());
    }

    log.info(BenchmarkConstants.SINGLE_LINE);
    log.info("Workload Histograms:\n{}", sb);
    log.info(BenchmarkConstants.SINGLE_LINE);
  }

  /**
   * Generate JSON histograms
   *
   * @param results Benchmark results
   * @return JSON string
   */
  public String writeJSONHistograms(Results results) {
    Map<String, JSONSerializable> map = new HashMap<>();
    map.put("completed", results.getSuccess());
    map.put("aborted", results.getAbort());
    map.put("rejected", results.getRetry());
    map.put("unexpected", results.getError());
    return JSONUtil.toJSONString(map);
  }

  private void writeRawResults(ResultWriter rw, ResultsConfiguration config, String baseFileName)
      throws Exception {
    String rawFileName = baseFileName + ".raw.csv";
    try (PrintStream ps =
        new PrintStream(FileUtil.joinPath(config.getOutputDirectory(), rawFileName))) {
      log.info("Output Raw data into file: {}", rawFileName);
      rw.writeRaw(config.getActiveTxTypes(), ps);
    }
  }

  private void writeSamples(ResultWriter rw, ResultsConfiguration config, String baseFileName)
      throws Exception {
    String sampleFileName = baseFileName + ".samples.csv";
    try (PrintStream ps =
        new PrintStream(FileUtil.joinPath(config.getOutputDirectory(), sampleFileName))) {
      log.info("Output samples into file: {}", sampleFileName);
      rw.writeSamples(ps);
    }
  }

  private void writeSummary(ResultWriter rw, ResultsConfiguration config, String baseFileName)
      throws Exception {
    String summaryFileName = baseFileName + ".summary.json";
    try (PrintStream ps =
        new PrintStream(FileUtil.joinPath(config.getOutputDirectory(), summaryFileName))) {
      log.info("Output summary data into file: {}", summaryFileName);
      rw.writeSummary(ps);
    }
  }

  private void writeParams(ResultWriter rw, ResultsConfiguration config, String baseFileName)
      throws Exception {
    String paramsFileName = baseFileName + ".params.json";
    try (PrintStream ps =
        new PrintStream(FileUtil.joinPath(config.getOutputDirectory(), paramsFileName))) {
      log.info("Output DBMS parameters into file: {}", paramsFileName);
      rw.writeParams(ps);
    }
  }

  private void writeMetrics(ResultWriter rw, ResultsConfiguration config, String baseFileName)
      throws Exception {
    String metricsFileName = baseFileName + ".metrics.json";
    try (PrintStream ps =
        new PrintStream(FileUtil.joinPath(config.getOutputDirectory(), metricsFileName))) {
      log.info("Output DBMS metrics into file: {}", metricsFileName);
      rw.writeMetrics(ps);
    }
  }

  private void writeConfig(ResultWriter rw, ResultsConfiguration config, String baseFileName)
      throws Exception {
    String configFileName = baseFileName + ".config.xml";
    try (PrintStream ps =
        new PrintStream(FileUtil.joinPath(config.getOutputDirectory(), configFileName))) {
      log.info("Output benchmark config into file: {}", configFileName);
      rw.writeConfig(ps);
    }
  }

  private void writeResultsCsv(ResultWriter rw, ResultsConfiguration config, String baseFileName)
      throws Exception {
    String resultsFileName = baseFileName + ".results.csv";
    try (PrintStream ps =
        new PrintStream(FileUtil.joinPath(config.getOutputDirectory(), resultsFileName))) {
      log.info(
          "Output results into file: {} with window size {}",
          resultsFileName,
          config.getWindowSize());
      rw.writeResults(config.getWindowSize(), ps);
    }
  }

  private void writePerTransactionResults(
      ResultWriter rw, ResultsConfiguration config, String baseFileName) throws Exception {
    for (TransactionType t : config.getActiveTxTypes()) {
      String fileName = baseFileName + ".results." + t.getName() + ".csv";
      try (PrintStream ps =
          new PrintStream(FileUtil.joinPath(config.getOutputDirectory(), fileName))) {
        rw.writeResults(config.getWindowSize(), ps, t);
      }
    }
  }
}
