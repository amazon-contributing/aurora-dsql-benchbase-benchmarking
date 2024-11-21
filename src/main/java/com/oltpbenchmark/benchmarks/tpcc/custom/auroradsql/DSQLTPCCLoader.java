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

package com.oltpbenchmark.benchmarks.tpcc.custom.auroradsql;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.pojo.District;
import com.oltpbenchmark.benchmarks.tpcc.pojo.History;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Item;
import com.oltpbenchmark.benchmarks.tpcc.pojo.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.OrderLine;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Stock;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Warehouse;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.ConnectionUtil;
import com.oltpbenchmark.util.SQLUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Custom DSQL TPC-C Benchmark Loader */
/*
 * Modifications:
 * - Auto retry with a new connection when a thread loading data
 *    into tables fails (set maxRetries in config.xml). This helps
 *    avoid any OCC errors. (OC001)
 * - Don't fail on duplicate key exceptions. Move to next statement.
 */
public final class DSQLTPCCLoader extends Loader<TPCCBenchmark> {

  private static final int FIRST_UNPROCESSED_O_ID = 2101;

  private static final int PROGRESS_BAR_LENGTH = 30;

  private final long numWarehouses;

  private final AtomicInteger warehousesLoaded;

  public DSQLTPCCLoader(TPCCBenchmark benchmark) {
    super(benchmark);
    numWarehouses = Math.max(Math.round(TPCCConfig.configWhseCount * this.scaleFactor), 1);
    warehousesLoaded = new AtomicInteger(0);
  }

  @Override
  public List<LoaderThread> createLoaderThreads() {
    List<LoaderThread> threads = new ArrayList<>();
    final CountDownLatch itemLatch = new CountDownLatch(1);
    final CountDownLatch warehouseLatch = new CountDownLatch((int) this.numWarehouses);

    // ITEM
    // This will be invoked first and executed in a single thread.
    threads.add(
        new LoaderThread(this.benchmark) {
          @Override
          public void load(Connection conn) {
            loadItems(conn, TPCCConfig.configItemCount);
          }

          @Override
          public void afterLoad() {
            itemLatch.countDown();
          }
        });

    // WAREHOUSES
    // We use a separate thread per warehouse. Each thread will load
    // all of the tables that depend on that warehouse. They all have
    // to wait until the ITEM table is loaded first though.
    for (int w = 1; w <= numWarehouses; w++) {
      final int w_id = w;
      LoaderThread t =
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {

              if (LOG.isDebugEnabled()) {
                LOG.debug("Starting to load WAREHOUSE {}", w_id);
              }
              // WAREHOUSE
              conn = loadWarehouse(conn, w_id);

              if (LOG.isDebugEnabled()) {
                LOG.debug("Starting to load STOCK {}", w_id);
              }
              // STOCK
              conn = loadStock(conn, w_id, TPCCConfig.configItemCount);

              if (LOG.isDebugEnabled()) {
                LOG.debug("Starting to load DISTRICT {}", w_id);
              }
              // DISTRICT
              conn = loadDistricts(conn, w_id, TPCCConfig.configDistPerWhse);

              if (LOG.isDebugEnabled()) {
                LOG.debug("Starting to load CUSTOMER {}", w_id);
              }
              // CUSTOMER
              conn =
                  loadCustomers(
                      conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

              if (LOG.isDebugEnabled()) {
                LOG.debug("Starting to load CUSTOMER HISTORY {}", w_id);
              }
              // CUSTOMER HISTORY
              conn =
                  loadCustomerHistory(
                      conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

              if (LOG.isDebugEnabled()) {
                LOG.debug("Starting to load ORDERS {}", w_id);
              }
              // ORDERS
              conn =
                  loadOpenOrders(
                      conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

              if (LOG.isDebugEnabled()) {
                LOG.debug("Starting to load NEW ORDERS {}", w_id);
              }
              // NEW ORDERS
              conn =
                  loadNewOrders(
                      conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

              if (LOG.isDebugEnabled()) {
                LOG.debug("Starting to load ORDER LINES {}", w_id);
              }
              // ORDER LINES
              loadOrderLines(
                  conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
            }

            @Override
            public void beforeLoad() {

              // Make sure that we load the ITEM table first

              try {
                itemLatch.await();
              } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
              }
            }

            @Override
            public void afterLoad() {
              warehouseLatch.countDown();
              logProgress(warehousesLoaded.incrementAndGet(), numWarehouses);
            }
          };
      threads.add(t);
    }

    // POST LOAD ANALYZE
    // This will run analyze on all the Tables in TPCC.
    threads.add(
        new LoaderThread(this.benchmark) {
          @Override
          public void load(Connection conn) {
            String[] tableNames =
                new String[] {
                  TPCCConstants.TABLENAME_ITEM,
                  TPCCConstants.TABLENAME_WAREHOUSE,
                  TPCCConstants.TABLENAME_STOCK,
                  TPCCConstants.TABLENAME_DISTRICT,
                  TPCCConstants.TABLENAME_CUSTOMER,
                  TPCCConstants.TABLENAME_HISTORY,
                  TPCCConstants.TABLENAME_OPENORDER,
                  TPCCConstants.TABLENAME_NEWORDER,
                  TPCCConstants.TABLENAME_ORDERLINE
                };
            LOG.info("Running ANALYZE on all tables...");
            runAnalyze(conn, tableNames);
          }

          @Override
          public void beforeLoad() {
            // Make sure that we load the all the warehouses and their data first
            try {
              warehouseLatch.await();
            } catch (InterruptedException ex) {
              throw new RuntimeException(ex);
            }
          }

          @Override
          public void afterLoad() {
            LOG.info("ANALYZE complete!");
          }
        });

    return (threads);
  }

  private PreparedStatement getInsertStatement(Connection conn, String tableName)
      throws SQLException {
    Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
    String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
    return conn.prepareStatement(sql);
  }

  /*
   * This custom function keeps retrying the data inserts until
   * the load succeeds or the attempts reach the max retry count.
   * Since new connections are created at each retry attempt, the
   * function returns the last successful connection to the db in
   * order to allow the next loader in the same thread to re-use it.
   */
  private Connection executeInsertStatmentWithRetry(
      Connection conn, Consumer<PreparedStatement> insertCallable, String tableName) {
    int attempts = 0;
    PreparedStatement stmt;
    while (attempts <= this.workConf.getMaxRetries()) {
      try {
        stmt = getInsertStatement(conn, tableName);
        insertCallable.accept(stmt);
        return conn;
      } catch (Exception e) {
        final Throwable t = e.getCause();
        if (isDuplicateKeyException(t)) {
          LOG.warn("Skipping insert due to duplicate key violation");
          return conn;
        }

        attempts++;
        if (attempts >= this.workConf.getMaxRetries()) {
          throw new RuntimeException("Load attempts exhausted", e);
        }

        LOG.warn("[Attempt: " + attempts + "]Batch insert failed with exception, retrying...");

        // Wait before retrying
        try {
          // Exponential delay with jitter
          long delay = calExpDelay(attempts);
          Thread.sleep(delay);
        } catch (InterruptedException ie) {
          throw new RuntimeException("Interrupted while retrying data load ", ie);
        }

        // Replace old Connection with new Connection
        // And Close previous connection and prepared statement
        try {
          Connection newConnection = ConnectionUtil.makeConnectionWithRetry(this.benchmark);

          if (!conn.isClosed()) {
            getInsertStatement(conn, tableName).close();
            conn.close();
          }

          conn = newConnection;
        } catch (SQLException se) {
          throw new RuntimeException("Failed to create connection while retrying data load ", se);
        }
      }
    }
    return conn;
  }

  /*
   * This custom function keeps retrying simple statements until
   * the load succeeds or the attempts reach the max retry count.
   * Since new connections are created at each retry attempt, the
   * function returns the last successful connection to the db in
   * order to allow the next loader in the same thread to re-use it.
   */
  private Connection executeStatmentWithRetry(Connection conn, Consumer<Statement> callable) {
    int attempts = 0;
    Statement stmt;
    while (attempts <= this.workConf.getMaxRetries()) {
      try {
        stmt = conn.createStatement();
        callable.accept(stmt);
        return conn;
      } catch (Exception e) {
        final Throwable t = e.getCause();
        if (isDuplicateKeyException(t)) {
          LOG.warn("Skipping sql execution due to duplicate key violation");
          return conn;
        }

        attempts++;
        if (attempts >= this.workConf.getMaxRetries()) {
          throw new RuntimeException("Execution attempts exhausted", e);
        }

        LOG.warn(
            "[Attempt: "
                + attempts
                + "]SQL statement execution failed with exception, retrying...");

        // Wait before retrying
        try {
          // Exponential delay with jitter
          long delay = calExpDelay(attempts);
          Thread.sleep(delay);
        } catch (InterruptedException ie) {
          throw new RuntimeException("Interrupted while retrying SQL statement execution", ie);
        }

        // Replace old Connection with new Connection
        // And Close previous connection and prepared statement
        try {
          Connection newConnection = ConnectionUtil.makeConnectionWithRetry(this.benchmark);

          if (!conn.isClosed()) {
            conn.close();
          }

          conn = newConnection;
        } catch (SQLException se) {
          throw new RuntimeException(
              "Failed to create connection while retrying SQL statement ", se);
        }
      }
    }
    return conn;
  }

  private static long calExpDelay(int attempts) {
    long baseDelay = 1000; // in milliseconds
    double jitterFactor = 1.0;

    long delay = (long) (baseDelay * Math.pow(2, attempts));
    delay = (long) (delay * (1 + jitterFactor * Math.random()));
    delay = Math.min(delay, 4000);

    return delay;
  }

  private boolean isDuplicateKeyException(Throwable t) {
    if (t.getMessage() != null && t.getMessage().contains("duplicate")) {
      return true;
    } else if (t.getCause() != null) {
      return isDuplicateKeyException(t.getCause());
    }
    return false;
  }

  private void logProgress(int warehouseLoaded, long numWarehouse) {
    double progess = (double) warehouseLoaded / (double) numWarehouse;
    progess = Math.max(0, Math.min(1, progess));

    int filled = (int) (PROGRESS_BAR_LENGTH * progess);
    StringBuilder bar = new StringBuilder();

    for (int i = 0; i < PROGRESS_BAR_LENGTH; i++) {
      if (i < filled) {
        bar.append("█");
      } else {
        bar.append("-");
      }
    }
    LOG.info(
        String.format(
            "Load Progress:\t[%s] %d/%d %s",
            bar.toString(),
            warehouseLoaded,
            numWarehouse,
            numWarehouse > 1 ? "warehouses" : "warehouse"));
  }

  protected Connection loadItems(Connection conn, int itemCount) {
    List<Item> items = new ArrayList<>();
    for (int i = 1; i <= itemCount; i++) {

      Item item = new Item();
      item.i_id = i;
      item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, benchmark.rng()));
      item.i_price = TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0;

      // i_data
      int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
      int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
      if (randPct > 10) {
        // 90% of time i_data isa random string of length [26 .. 50]
        item.i_data = TPCCUtil.randomStr(len);
      } else {
        // 10% of time i_data has "ORIGINAL" crammed somewhere in
        // middle
        int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
        item.i_data =
            TPCCUtil.randomStr(startORIGINAL - 1)
                + "ORIGINAL"
                + TPCCUtil.randomStr(len - startORIGINAL - 9);
      }

      item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

      items.add(item);

      if (items.size() == workConf.getBatchSize()) {
        conn =
            executeInsertStatmentWithRetry(
                conn,
                (stmt) -> {
                  insertItems(items, stmt);
                },
                TPCCConstants.TABLENAME_ITEM);
        items.clear();
      }
    }

    if (!items.isEmpty()) {
      conn =
          executeInsertStatmentWithRetry(
              conn,
              (stmt) -> {
                insertItems(items, stmt);
              },
              TPCCConstants.TABLENAME_ITEM);
      items.clear();
    }
    return conn;
  }

  private void insertItems(List<Item> items, PreparedStatement itemPrepStmt) {
    try {
      for (Item item : items) {
        int idx = 1;
        itemPrepStmt.setLong(idx++, item.i_id);
        itemPrepStmt.setString(idx++, item.i_name);
        itemPrepStmt.setDouble(idx++, item.i_price);
        itemPrepStmt.setString(idx++, item.i_data);
        itemPrepStmt.setLong(idx, item.i_im_id);
        itemPrepStmt.addBatch();
      }
      itemPrepStmt.executeBatch();
      itemPrepStmt.clearBatch();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to insert items", e);
    }
  }

  protected Connection loadWarehouse(Connection conn, int w_id) {

    Warehouse warehouse = new Warehouse();

    warehouse.w_id = w_id;
    warehouse.w_ytd = 300000;

    // random within [0.0000 .. 0.2000]
    warehouse.w_tax = (TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0;
    warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
    warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
    warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
    warehouse.w_zip = "123456789";

    conn =
        executeInsertStatmentWithRetry(
            conn,
            (stmt) -> {
              insertWarehouse(warehouse, stmt);
            },
            TPCCConstants.TABLENAME_WAREHOUSE);

    return conn;
  }

  private void insertWarehouse(Warehouse warehouse, PreparedStatement whsePrepStmt) {
    try {
      int idx = 1;
      whsePrepStmt.setLong(idx++, warehouse.w_id);
      whsePrepStmt.setDouble(idx++, warehouse.w_ytd);
      whsePrepStmt.setDouble(idx++, warehouse.w_tax);
      whsePrepStmt.setString(idx++, warehouse.w_name);
      whsePrepStmt.setString(idx++, warehouse.w_street_1);
      whsePrepStmt.setString(idx++, warehouse.w_street_2);
      whsePrepStmt.setString(idx++, warehouse.w_city);
      whsePrepStmt.setString(idx++, warehouse.w_state);
      whsePrepStmt.setString(idx, warehouse.w_zip);
      whsePrepStmt.execute();
    } catch (SQLException sqlException) {
      throw new RuntimeException("Failed to insert warehouse", sqlException);
    }
  }

  protected Connection loadStock(Connection conn, int w_id, int numItems) {

    List<Stock> stocks = new ArrayList<>();

    for (int i = 1; i <= numItems; i++) {
      Stock stock = new Stock();
      stock.s_i_id = i;
      stock.s_w_id = w_id;
      stock.s_quantity = TPCCUtil.randomNumber(10, 100, benchmark.rng());
      stock.s_ytd = 0;
      stock.s_order_cnt = 0;
      stock.s_remote_cnt = 0;

      // s_data
      int randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
      int len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
      if (randPct > 10) {
        // 90% of time i_data isa random string of length [26 ..
        // 50]
        stock.s_data = TPCCUtil.randomStr(len);
      } else {
        // 10% of time i_data has "ORIGINAL" crammed somewhere
        // in middle
        int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
        stock.s_data =
            TPCCUtil.randomStr(startORIGINAL - 1)
                + "ORIGINAL"
                + TPCCUtil.randomStr(len - startORIGINAL - 9);
      }
      stocks.add(stock);

      if (stocks.size() == workConf.getBatchSize()) {
        conn =
            executeInsertStatmentWithRetry(
                conn,
                (stmt) -> {
                  insertStock(stocks, stmt);
                },
                TPCCConstants.TABLENAME_STOCK);
        stocks.clear();
      }
    }

    if (!stocks.isEmpty()) {
      conn =
          executeInsertStatmentWithRetry(
              conn,
              (stmt) -> {
                insertStock(stocks, stmt);
              },
              TPCCConstants.TABLENAME_STOCK);
      stocks.clear();
    }
    return conn;
  }

  private void insertStock(List<Stock> stocks, PreparedStatement stockPreparedStatement) {
    try {
      for (Stock stock : stocks) {
        int idx = 1;
        stockPreparedStatement.setLong(idx++, stock.s_w_id);
        stockPreparedStatement.setLong(idx++, stock.s_i_id);
        stockPreparedStatement.setLong(idx++, stock.s_quantity);
        stockPreparedStatement.setDouble(idx++, stock.s_ytd);
        stockPreparedStatement.setLong(idx++, stock.s_order_cnt);
        stockPreparedStatement.setLong(idx++, stock.s_remote_cnt);
        stockPreparedStatement.setString(idx++, stock.s_data);
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx++, TPCCUtil.randomStr(24));
        stockPreparedStatement.setString(idx, TPCCUtil.randomStr(24));
        stockPreparedStatement.addBatch();
      }

      stockPreparedStatement.executeBatch();
      stockPreparedStatement.clearBatch();
    } catch (SQLException sqlException) {
      throw new RuntimeException("Failed to insert stocks", sqlException);
    }
  }

  protected Connection loadDistricts(Connection conn, int w_id, int districtsPerWarehouse) {
    for (int d = 1; d <= districtsPerWarehouse; d++) {
      District district = new District();
      district.d_id = d;
      district.d_w_id = w_id;
      district.d_ytd = 30000;

      // random within [0.0000 .. 0.2000]
      district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0);

      district.d_next_o_id = TPCCConfig.configCustPerDist + 1;
      district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
      district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
      district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
      district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
      district.d_state = TPCCUtil.randomStr(3).toUpperCase();
      district.d_zip = "123456789";

      conn =
          executeInsertStatmentWithRetry(
              conn,
              (stmt) -> {
                insertDistrict(district, stmt);
              },
              TPCCConstants.TABLENAME_DISTRICT);
    }
    return conn;
  }

  private void insertDistrict(District district, PreparedStatement distPrepStmt) {
    try {
      int idx = 1;
      distPrepStmt.setLong(idx++, district.d_w_id);
      distPrepStmt.setLong(idx++, district.d_id);
      distPrepStmt.setDouble(idx++, district.d_ytd);
      distPrepStmt.setDouble(idx++, district.d_tax);
      distPrepStmt.setLong(idx++, district.d_next_o_id);
      distPrepStmt.setString(idx++, district.d_name);
      distPrepStmt.setString(idx++, district.d_street_1);
      distPrepStmt.setString(idx++, district.d_street_2);
      distPrepStmt.setString(idx++, district.d_city);
      distPrepStmt.setString(idx++, district.d_state);
      distPrepStmt.setString(idx, district.d_zip);
      distPrepStmt.executeUpdate();
    } catch (SQLException sqlException) {
      throw new RuntimeException("Failed to insert districts", sqlException);
    }
  }

  protected Connection loadCustomers(
      Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

    List<Customer> customers = new ArrayList<>();

    for (int d = 1; d <= districtsPerWarehouse; d++) {
      for (int c = 1; c <= customersPerDistrict; c++) {
        Timestamp sysdate = new Timestamp(System.currentTimeMillis());

        Customer customer = new Customer();
        customer.c_id = c;
        customer.c_d_id = d;
        customer.c_w_id = w_id;

        // discount is random between [0.0000 ... 0.5000]
        customer.c_discount = (float) (TPCCUtil.randomNumber(1, 5000, benchmark.rng()) / 10000.0);

        if (TPCCUtil.randomNumber(1, 100, benchmark.rng()) <= 10) {
          customer.c_credit = "BC"; // 10% Bad Credit
        } else {
          customer.c_credit = "GC"; // 90% Good Credit
        }
        if (c <= 1000) {
          customer.c_last = TPCCUtil.getLastName(c - 1);
        } else {
          customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(benchmark.rng());
        }
        customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, benchmark.rng()));
        customer.c_credit_lim = 50000;

        customer.c_balance = -10;
        customer.c_ytd_payment = 10;
        customer.c_payment_cnt = 1;
        customer.c_delivery_cnt = 0;

        customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
        customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
        customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
        customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
        // TPC-C 4.3.2.7: 4 random digits + "11111"
        customer.c_zip = TPCCUtil.randomNStr(4) + "11111";
        customer.c_phone = TPCCUtil.randomNStr(16);
        customer.c_since = sysdate;
        customer.c_middle = "OE";
        customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, benchmark.rng()));

        customers.add(customer);

        if (customers.size() == workConf.getBatchSize()) {
          conn =
              executeInsertStatmentWithRetry(
                  conn,
                  (stmt) -> {
                    insertCustomer(customers, stmt);
                  },
                  TPCCConstants.TABLENAME_CUSTOMER);
          customers.clear();
        }
      }
    }

    if (!customers.isEmpty()) {
      conn =
          executeInsertStatmentWithRetry(
              conn,
              (stmt) -> {
                insertCustomer(customers, stmt);
              },
              TPCCConstants.TABLENAME_CUSTOMER);
      customers.clear();
    }

    return conn;
  }

  private void insertCustomer(List<Customer> customers, PreparedStatement custPrepStmt) {
    try {
      for (Customer customer : customers) {
        int idx = 1;
        custPrepStmt.setLong(idx++, customer.c_w_id);
        custPrepStmt.setLong(idx++, customer.c_d_id);
        custPrepStmt.setLong(idx++, customer.c_id);
        custPrepStmt.setDouble(idx++, customer.c_discount);
        custPrepStmt.setString(idx++, customer.c_credit);
        custPrepStmt.setString(idx++, customer.c_last);
        custPrepStmt.setString(idx++, customer.c_first);
        custPrepStmt.setDouble(idx++, customer.c_credit_lim);
        custPrepStmt.setDouble(idx++, customer.c_balance);
        custPrepStmt.setDouble(idx++, customer.c_ytd_payment);
        custPrepStmt.setLong(idx++, customer.c_payment_cnt);
        custPrepStmt.setLong(idx++, customer.c_delivery_cnt);
        custPrepStmt.setString(idx++, customer.c_street_1);
        custPrepStmt.setString(idx++, customer.c_street_2);
        custPrepStmt.setString(idx++, customer.c_city);
        custPrepStmt.setString(idx++, customer.c_state);
        custPrepStmt.setString(idx++, customer.c_zip);
        custPrepStmt.setString(idx++, customer.c_phone);
        custPrepStmt.setTimestamp(idx++, customer.c_since);
        custPrepStmt.setString(idx++, customer.c_middle);
        custPrepStmt.setString(idx, customer.c_data);
        custPrepStmt.addBatch();
      }

      custPrepStmt.executeBatch();
      custPrepStmt.clearBatch();
    } catch (SQLException sqlException) {
      throw new RuntimeException("Failed to insert customers", sqlException);
    }
  }

  protected Connection loadCustomerHistory(
      Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

    List<History> historyList = new ArrayList<>();

    for (int d = 1; d <= districtsPerWarehouse; d++) {
      for (int c = 1; c <= customersPerDistrict; c++) {
        Timestamp sysdate = new Timestamp(System.currentTimeMillis());

        History history = new History();
        history.h_c_id = c;
        history.h_c_d_id = d;
        history.h_c_w_id = w_id;
        history.h_d_id = d;
        history.h_w_id = w_id;
        history.h_date = sysdate;
        history.h_amount = 10;
        history.h_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, benchmark.rng()));

        historyList.add(history);

        if (historyList.size() == workConf.getBatchSize()) {
          conn =
              executeInsertStatmentWithRetry(
                  conn,
                  (stmt) -> {
                    insertCustomerHistory(historyList, stmt);
                  },
                  TPCCConstants.TABLENAME_HISTORY);
          historyList.clear();
        }
      }
    }

    if (!historyList.isEmpty()) {
      conn =
          executeInsertStatmentWithRetry(
              conn,
              (stmt) -> {
                insertCustomerHistory(historyList, stmt);
              },
              TPCCConstants.TABLENAME_HISTORY);
      historyList.clear();
    }

    return conn;
  }

  private void insertCustomerHistory(List<History> historyList, PreparedStatement histPrepStmt) {
    try {
      for (History history : historyList) {
        int idx = 1;
        histPrepStmt.setInt(idx++, history.h_c_id);
        histPrepStmt.setInt(idx++, history.h_c_d_id);
        histPrepStmt.setInt(idx++, history.h_c_w_id);
        histPrepStmt.setInt(idx++, history.h_d_id);
        histPrepStmt.setInt(idx++, history.h_w_id);
        histPrepStmt.setTimestamp(idx++, history.h_date);
        histPrepStmt.setDouble(idx++, history.h_amount);
        histPrepStmt.setString(idx, history.h_data);
        histPrepStmt.addBatch();
      }

      histPrepStmt.executeBatch();
      histPrepStmt.clearBatch();
    } catch (SQLException sqlException) {
      throw new RuntimeException("Failed to insert history", sqlException);
    }
  }

  protected Connection loadOpenOrders(
      Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

    List<Oorder> oorders = new ArrayList<>();

    for (int d = 1; d <= districtsPerWarehouse; d++) {
      // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
      int[] c_ids = new int[customersPerDistrict];
      for (int i = 0; i < customersPerDistrict; ++i) {
        c_ids[i] = i + 1;
      }
      // Collections.shuffle exists, but there is no
      // Arrays.shuffle
      for (int i = 0; i < c_ids.length - 1; ++i) {
        int remaining = c_ids.length - i - 1;
        int swapIndex = benchmark.rng().nextInt(remaining) + i + 1;

        int temp = c_ids[swapIndex];
        c_ids[swapIndex] = c_ids[i];
        c_ids[i] = temp;
      }

      for (int c = 1; c <= customersPerDistrict; c++) {

        Oorder oorder = new Oorder();
        oorder.o_id = c;
        oorder.o_w_id = w_id;
        oorder.o_d_id = d;
        oorder.o_c_id = c_ids[c - 1];
        // o_carrier_id is set *only* for orders with ids < 2101
        // [4.3.3.1]
        if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
          oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, benchmark.rng());
        } else {
          oorder.o_carrier_id = null;
        }
        oorder.o_ol_cnt = getRandomCount(w_id, c, d);
        oorder.o_all_local = 1;
        oorder.o_entry_d = new Timestamp(System.currentTimeMillis());

        oorders.add(oorder);

        if (oorders.size() == workConf.getBatchSize()) {
          conn =
              executeInsertStatmentWithRetry(
                  conn,
                  (stmt) -> {
                    insertOpenOrders(oorders, stmt);
                  },
                  TPCCConstants.TABLENAME_OPENORDER);
          oorders.clear();
        }
      }
    }

    if (!oorders.isEmpty()) {
      conn =
          executeInsertStatmentWithRetry(
              conn,
              (stmt) -> {
                insertOpenOrders(oorders, stmt);
              },
              TPCCConstants.TABLENAME_OPENORDER);
      oorders.clear();
    }

    return conn;
  }

  private void insertOpenOrders(List<Oorder> oorders, PreparedStatement openOrderStatement) {
    try {
      for (Oorder oorder : oorders) {
        int idx = 1;
        openOrderStatement.setInt(idx++, oorder.o_w_id);
        openOrderStatement.setInt(idx++, oorder.o_d_id);
        openOrderStatement.setInt(idx++, oorder.o_id);
        openOrderStatement.setInt(idx++, oorder.o_c_id);
        if (oorder.o_carrier_id != null) {
          openOrderStatement.setInt(idx++, oorder.o_carrier_id);
        } else {
          openOrderStatement.setNull(idx++, Types.INTEGER);
        }
        openOrderStatement.setInt(idx++, oorder.o_ol_cnt);
        openOrderStatement.setInt(idx++, oorder.o_all_local);
        openOrderStatement.setTimestamp(idx, oorder.o_entry_d);
        openOrderStatement.addBatch();
      }

      openOrderStatement.executeBatch();
      openOrderStatement.clearBatch();
    } catch (SQLException sqlException) {
      throw new RuntimeException("Failed to insert open orders", sqlException);
    }
  }

  private int getRandomCount(int w_id, int c, int d) {
    Customer customer = new Customer();
    customer.c_id = c;
    customer.c_d_id = d;
    customer.c_w_id = w_id;

    Random random = new Random(customer.hashCode());

    return TPCCUtil.randomNumber(5, 15, random);
  }

  protected Connection loadNewOrders(
      Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

    List<NewOrder> newOrders = new ArrayList<>();

    for (int d = 1; d <= districtsPerWarehouse; d++) {

      for (int c = 1; c <= customersPerDistrict; c++) {

        // 900 rows in the NEW-ORDER table corresponding to the last
        // 900 rows in the ORDER table for that district (i.e.,
        // with NO_O_ID between 2,101 and 3,000)
        if (c >= FIRST_UNPROCESSED_O_ID) {
          NewOrder new_order = new NewOrder();
          new_order.no_w_id = w_id;
          new_order.no_d_id = d;
          new_order.no_o_id = c;

          newOrders.add(new_order);
        }

        if (newOrders.size() == workConf.getBatchSize()) {
          conn =
              executeInsertStatmentWithRetry(
                  conn,
                  (stmt) -> {
                    insertNewOrder(newOrders, stmt);
                  },
                  TPCCConstants.TABLENAME_NEWORDER);
          newOrders.clear();
        }
      }
    }

    if (!newOrders.isEmpty()) {
      conn =
          executeInsertStatmentWithRetry(
              conn,
              (stmt) -> {
                insertNewOrder(newOrders, stmt);
              },
              TPCCConstants.TABLENAME_NEWORDER);
      newOrders.clear();
    }

    return conn;
  }

  private void insertNewOrder(List<NewOrder> newOrders, PreparedStatement newOrderStatement) {
    try {
      for (NewOrder newOrder : newOrders) {
        int idx = 1;
        newOrderStatement.setInt(idx++, newOrder.no_w_id);
        newOrderStatement.setInt(idx++, newOrder.no_d_id);
        newOrderStatement.setInt(idx, newOrder.no_o_id);
        newOrderStatement.addBatch();
      }

      newOrderStatement.executeBatch();
      newOrderStatement.clearBatch();
    } catch (SQLException sqlException) {
      throw new RuntimeException("Failed to insert new orders", sqlException);
    }
  }

  protected Connection loadOrderLines(
      Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

    List<OrderLine> orderLines = new ArrayList<>();

    for (int d = 1; d <= districtsPerWarehouse; d++) {

      for (int c = 1; c <= customersPerDistrict; c++) {

        int count = getRandomCount(w_id, c, d);

        for (int l = 1; l <= count; l++) {
          OrderLine order_line = new OrderLine();
          order_line.ol_w_id = w_id;
          order_line.ol_d_id = d;
          order_line.ol_o_id = c;
          order_line.ol_number = l; // ol_number
          order_line.ol_i_id =
              TPCCUtil.randomNumber(1, TPCCConfig.configItemCount, benchmark.rng());
          if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
            order_line.ol_delivery_d = new Timestamp(System.currentTimeMillis());
            order_line.ol_amount = 0;
          } else {
            order_line.ol_delivery_d = null;
            // random within [0.01 .. 9,999.99]
            order_line.ol_amount =
                (float) (TPCCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
          }
          order_line.ol_supply_w_id = order_line.ol_w_id;
          order_line.ol_quantity = 5;
          order_line.ol_dist_info = TPCCUtil.randomStr(24);

          orderLines.add(order_line);

          if (orderLines.size() == workConf.getBatchSize()) {
            conn =
                executeInsertStatmentWithRetry(
                    conn,
                    (stmt) -> {
                      insertOrderLine(orderLines, stmt);
                    },
                    TPCCConstants.TABLENAME_ORDERLINE);
            orderLines.clear();
          }
        }
      }
    }

    if (!orderLines.isEmpty()) {
      conn =
          executeInsertStatmentWithRetry(
              conn,
              (stmt) -> {
                insertOrderLine(orderLines, stmt);
              },
              TPCCConstants.TABLENAME_ORDERLINE);
      orderLines.clear();
    }

    return conn;
  }

  private void insertOrderLine(List<OrderLine> orderLines, PreparedStatement orderLineStatement) {
    try {
      for (OrderLine orderLine : orderLines) {
        int idx = 1;
        orderLineStatement.setInt(idx++, orderLine.ol_w_id);
        orderLineStatement.setInt(idx++, orderLine.ol_d_id);
        orderLineStatement.setInt(idx++, orderLine.ol_o_id);
        orderLineStatement.setInt(idx++, orderLine.ol_number);
        orderLineStatement.setLong(idx++, orderLine.ol_i_id);
        if (orderLine.ol_delivery_d != null) {
          orderLineStatement.setTimestamp(idx++, orderLine.ol_delivery_d);
        } else {
          orderLineStatement.setNull(idx++, 0);
        }
        orderLineStatement.setDouble(idx++, orderLine.ol_amount);
        orderLineStatement.setLong(idx++, orderLine.ol_supply_w_id);
        orderLineStatement.setDouble(idx++, orderLine.ol_quantity);
        orderLineStatement.setString(idx, orderLine.ol_dist_info);
        orderLineStatement.addBatch();
      }

      orderLineStatement.executeBatch();
      orderLineStatement.clearBatch();
    } catch (SQLException sqlException) {
      throw new RuntimeException("Failed to insert orderline", sqlException);
    }
  }

  private Connection runAnalyze(Connection conn, String[] tableNames) {
    for (String tableName : tableNames) {
      conn =
          executeStatmentWithRetry(
              conn,
              (stmt) -> {
                analyzeTables(stmt, tableName);
              });
    }
    return conn;
  }

  private void analyzeTables(Statement stmt, String tableName) {
    try {
      stmt.execute("ANALYZE " + tableName);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to run ANALYZE on table: " + tableName);
    }
  }
}
