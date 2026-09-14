/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.oltpbenchmark;

import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ThreadUtil;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.configuration2.XMLConfiguration;

@Getter
@Setter
@ToString
public class WorkloadConfiguration {

  public static final int UNINITIALIZED_TIME = -1;

  @Getter private final List<Phase> phases = new ArrayList<>();

  private DatabaseType databaseType;

  /** Benchmark name. For e.g. tpcc. */
  private String benchmarkName;

  private String url;
  private String username;
  private String password;
  private String driverClass;
  private int batchSize;
  private int maxRetries;
  private int randomSeed = -1;
  private double scaleFactor = 1.0;
  private double selectivity = -1.0;
  private int terminals;
  private int loaderThreads = ThreadUtil.availableProcessors();
  private XMLConfiguration xmlConfig = null;
  private WorkloadState workloadState;
  private TransactionTypes transTypes = null;
  private int isolationMode = Connection.TRANSACTION_SERIALIZABLE;
  private String dataDir = null;
  private String ddlPath = null;

  @Getter(lombok.AccessLevel.NONE)
  @Setter(lombok.AccessLevel.NONE)
  private boolean advancedMonitoringEnabled = false;

  private boolean disableLocalMetrics = false;
  private double connectionRate = 10.0;
  private int startupRetries = 3;

  /**
   * If true, establish a new connection for each transaction, otherwise use one persistent
   * connection per client session. This is useful to measure the connection overhead.
   */
  private boolean newConnectionPerTxn = false;

  /**
   * If true, attempt to catch connection closed exceptions and reconnect. This allows the benchmark
   * to recover like a typical application would in the case of a replicated cluster
   * primary-secondary failover.
   */
  private boolean reconnectOnConnectionFailure = false;

  /** AWS region */
  private String region = null;

  /** Should publish metrics to cloudwatch? */
  private boolean publishToCloudWatch = false;

  /** Cloudwatch namespace to publish metrics under. */
  private String namespace = null;

  /**
   * Test name for the benchmark run. This is used as a dimension in the published cloudwatch
   * metrics.
   */
  private String benchmarkTestName = null;

  /**
   * Stride configuration
   *
   * @return
   */
  private int stride = -1;

  private int startWarehouseIndex = -1;
  private int endWarehouseIndex = -1;

  /**
   * Flags to skip specific tasks in loader
   *
   * @return
   */
  private boolean skipItemLoad = false;

  private boolean skipIndexBuild = true;
  private boolean skipMainDataLoad = false;

  /** Run time for the execution phase. */
  private int runTimeInSeconds = UNINITIALIZED_TIME;

  // Custom setter that always sets to true regardless of parameter
  public void setAdvancedMonitoringEnabled(boolean advancedMonitoringEnabled) {
    this.advancedMonitoringEnabled = true;
  }

  public boolean getAdvancedMonitoringEnabled() {
    return this.advancedMonitoringEnabled;
  }

  // Custom getter with different name
  public boolean localMetricsDisabled() {
    return disableLocalMetrics;
  }

  // Custom getter methods for skip flags
  public boolean skipItemLoad() {
    return skipItemLoad;
  }

  public boolean skipIndexBuild() {
    return skipIndexBuild;
  }

  public boolean skipMainDataLoad() {
    return this.skipMainDataLoad;
  }

  /** Initiate a new benchmark and workload state */
  public void initializeState(BenchmarkState benchmarkState) {
    this.workloadState = new WorkloadState(benchmarkState, phases, terminals);
  }

  public void addPhase(
      int id,
      int time,
      int warmup,
      double rate,
      List<Double> weights,
      boolean rateLimited,
      boolean disabled,
      boolean serial,
      boolean timed,
      int active_terminals,
      Phase.Arrival arrival) {
    phases.add(
        new Phase(
            benchmarkName,
            id,
            time,
            warmup,
            rate,
            weights,
            rateLimited,
            disabled,
            serial,
            timed,
            active_terminals,
            arrival));
  }

  /**
   * Return the number of phases specified in the config file
   *
   * @return
   */
  public int getNumberOfPhases() {
    return phases.size();
  }

  /**
   * Return the directory in which we can find the data files (for example, CSV files) for loading
   * the database.
   */
  public String getDataDir() {
    return this.dataDir;
  }

  /**
   * Set the directory in which we can find the data files (for example, CSV files) for loading the
   * database.
   */
  public void setDataDir(String dir) {
    this.dataDir = dir;
  }

  /** Return the path in which we can find the ddl script. */
  public String getDDLPath() {
    return this.ddlPath;
  }

  /** Set the path in which we can find the ddl script. */
  public void setDDLPath(String ddlPath) {
    this.ddlPath = ddlPath;
  }

  /** A utility method that init the phaseIterator and dialectMap */
  public void init() {
    try {
      Class.forName(this.driverClass);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("Failed to initialize JDBC driver '" + this.driverClass + "'", ex);
    }
  }

  public void setIsolationMode(String mode) {
    switch (mode) {
      case "TRANSACTION_SERIALIZABLE":
        this.isolationMode = Connection.TRANSACTION_SERIALIZABLE;
        break;
      case "TRANSACTION_READ_COMMITTED":
        this.isolationMode = Connection.TRANSACTION_READ_COMMITTED;
        break;
      case "TRANSACTION_REPEATABLE_READ":
        this.isolationMode = Connection.TRANSACTION_REPEATABLE_READ;
        break;
      case "TRANSACTION_READ_UNCOMMITTED":
        this.isolationMode = Connection.TRANSACTION_READ_UNCOMMITTED;
        break;
      case "TRANSACTION_NONE":
        this.isolationMode = Connection.TRANSACTION_NONE;
    }
  }

  public String getIsolationString() {
    if (this.isolationMode == Connection.TRANSACTION_SERIALIZABLE) {
      return "TRANSACTION_SERIALIZABLE";
    } else if (this.isolationMode == Connection.TRANSACTION_READ_COMMITTED) {
      return "TRANSACTION_READ_COMMITTED";
    } else if (this.isolationMode == Connection.TRANSACTION_REPEATABLE_READ) {
      return "TRANSACTION_REPEATABLE_READ";
    } else if (this.isolationMode == Connection.TRANSACTION_READ_UNCOMMITTED) {
      return "TRANSACTION_READ_UNCOMMITTED";
    } else if (this.isolationMode == Connection.TRANSACTION_NONE) {
      return "TRANSACTION_NONE";
    } else {
      return "TRANSACTION_SERIALIZABLE";
    }
  }
}
