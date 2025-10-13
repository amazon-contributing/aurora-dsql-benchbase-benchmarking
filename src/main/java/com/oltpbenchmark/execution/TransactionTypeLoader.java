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
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.XMLConfiguration;

/** Handles loading transaction types from configuration. */
@Slf4j
public class TransactionTypeLoader {

  /**
   * Load transaction types for a benchmark
   *
   * @param xmlConfig XML configuration
   * @param pluginXpathString Plugin xpath string for XML queries
   * @param bench Benchmark module
   * @param lastTxnId Last transaction ID from previous benchmarks
   * @return Transaction types and list of active transaction types
   */
  public LoaderResult loadTransactionTypes(
      XMLConfiguration xmlConfig, String pluginXpathString, BenchmarkModule bench, int lastTxnId) {

    List<TransactionType> ttypes = new ArrayList<>();
    List<TransactionType> activeTXTypes = new ArrayList<>();
    ttypes.add(TransactionType.INVALID);

    int numTxnTypes = getNumTransactionTypes(xmlConfig, pluginXpathString);
    int txnIdOffset = lastTxnId;

    for (int i = 1; i <= numTxnTypes; i++) {
      String key = "transactiontypes" + pluginXpathString + "/transactiontype[" + i + "]";
      TransactionType txnType = loadTransactionType(xmlConfig, key, bench, i, txnIdOffset);

      // Keep a reference for filtering
      activeTXTypes.add(txnType);

      // Add a ref for the active TTypes in this benchmark
      ttypes.add(txnType);
    }

    // Wrap the list of transactions
    TransactionTypes tt = new TransactionTypes(ttypes);
    log.debug("Using the following transaction types: {}", tt);

    return new LoaderResult(tt, activeTXTypes, lastTxnId + numTxnTypes);
  }

  /**
   * Load transaction groupings
   *
   * @param xmlConfig XML configuration
   * @param pluginXpathString Plugin xpath string
   * @param numTxnTypes Number of transaction types
   */
  public void loadTransactionGroupings(
      XMLConfiguration xmlConfig, String pluginXpathString, int numTxnTypes) {

    int numGroupings =
        xmlConfig
            .configurationsAt("transactiontypes" + pluginXpathString + "/groupings/grouping")
            .size();

    log.debug("Num groupings: {}", numGroupings);

    for (int i = 1; i < numGroupings + 1; i++) {
      String key = "transactiontypes" + pluginXpathString + "/groupings/grouping[" + i + "]";
      validateGrouping(xmlConfig, key, numTxnTypes);
    }
  }

  private int getNumTransactionTypes(XMLConfiguration xmlConfig, String pluginXpathString) {
    int numTxnTypes =
        xmlConfig
            .configurationsAt("transactiontypes" + pluginXpathString + "/transactiontype")
            .size();

    // if it is a single workload run, <transactiontypes /> w/o attribute is used
    if (numTxnTypes == 0) {
      String fallbackTest = "[not(@bench)]";
      numTxnTypes =
          xmlConfig.configurationsAt("transactiontypes" + fallbackTest + "/transactiontype").size();
    }

    return numTxnTypes;
  }

  private TransactionType loadTransactionType(
      XMLConfiguration xmlConfig, String key, BenchmarkModule bench, int index, int txnIdOffset) {

    String txnName = xmlConfig.getString(key + "/name");

    // Get ID if specified; else use index
    int txnId = index;
    if (xmlConfig.containsKey(key + "/id")) {
      txnId = xmlConfig.getInt(key + "/id");
    }

    long preExecutionWait = 0;
    if (xmlConfig.containsKey(key + "/preExecutionWait")) {
      preExecutionWait = xmlConfig.getLong(key + "/preExecutionWait");
    }

    long postExecutionWait = 0;
    if (xmlConfig.containsKey(key + "/postExecutionWait")) {
      postExecutionWait = xmlConfig.getLong(key + "/postExecutionWait");
    }

    return bench.initTransactionType(
        txnName, txnId + txnIdOffset, preExecutionWait, postExecutionWait);
  }

  private void validateGrouping(XMLConfiguration xmlConfig, String key, int numTxnTypes) {
    // Get the name for the grouping and make sure it's valid
    String groupingName = xmlConfig.getString(key + "/name").toLowerCase();

    if (!groupingName.matches("^[a-z]\\w*$")) {
      log.error(
          String.format(
              "Grouping name \"%s\" is invalid. Must begin with a letter and contain only"
                  + " alphanumeric characters.",
              groupingName));
      System.exit(-1);
    } else if (groupingName.equals("all")) {
      log.error("Grouping name \"all\" is reserved. Please pick a different name.");
      System.exit(-1);
    }

    // Get the weights for this grouping and make sure that there
    // is an appropriate number of them
    String[] groupingWeights = xmlConfig.getString(key + "/weights").split("\\s*,\\s*");

    if (groupingWeights.length != numTxnTypes) {
      log.error(
          String.format(
              "Grouping \"%s\" has %d weights, but there are %d transactions in this"
                  + " benchmark.",
              groupingName, groupingWeights.length, numTxnTypes));
      System.exit(-1);
    }

    log.debug(
        "Creating grouping with name, weights: {}, {}",
        groupingName,
        java.util.Arrays.toString(groupingWeights));
  }

  /** Result of loading transaction types */
  public static class LoaderResult {
    private final TransactionTypes transactionTypes;
    private final List<TransactionType> activeTxTypes;
    private final int lastTxnId;

    public LoaderResult(
        TransactionTypes transactionTypes, List<TransactionType> activeTxTypes, int lastTxnId) {
      this.transactionTypes = transactionTypes;
      this.activeTxTypes = activeTxTypes;
      this.lastTxnId = lastTxnId;
    }

    public TransactionTypes getTransactionTypes() {
      return transactionTypes;
    }

    public List<TransactionType> getActiveTxTypes() {
      return activeTxTypes;
    }

    public int getLastTxnId() {
      return lastTxnId;
    }
  }
}
