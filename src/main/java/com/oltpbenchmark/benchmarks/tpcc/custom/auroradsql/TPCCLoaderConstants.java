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

/**
 * Constants used by the DSQL TPCC Loader. Centralizes all constants to improve maintainability and
 * readability.
 */
public final class TPCCLoaderConstants {

  private TPCCLoaderConstants() {
    // Private constructor to prevent instantiation
  }

  // Transaction and Thread Names
  public static final String TX_NAME = "Loader";
  public static final String LOAD_ITEMS_THREAD_NAME = "loadItems";

  // Business Logic Constants
  public static final int FIRST_UNPROCESSED_O_ID = 2101;

  // SQL Queries
  public static final String CREATE_CUSTOMER_INDEX_ASYNC =
      "CREATE INDEX ASYNC idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first)";

  public static final String SELECT_JOB_STATUS = "SELECT status FROM sys.jobs WHERE job_id = ?";

  // Index Job Status
  public static final String JOB_STATUS_COMPLETED = "completed";
  public static final String JOB_STATUS_PROCESSING = "processing";

  // Thread Sleep Durations
  public static final long INDEX_CHECK_INTERVAL_MS = 1000;
  public static final long POST_INDEX_WAIT_MS = 1000;

  // String Constants for Data Generation
  public static final String ORIGINAL_STRING = "ORIGINAL";
  public static final String BAD_CREDIT = "BC";
  public static final String GOOD_CREDIT = "GC";
  public static final String MIDDLE_NAME = "OE";
  public static final String ZIP_SUFFIX = "11111";

  // Data Generation Limits
  public static final int ITEM_NAME_MIN_LENGTH = 14;
  public static final int ITEM_NAME_MAX_LENGTH = 24;
  public static final int ITEM_PRICE_MIN = 100;
  public static final int ITEM_PRICE_MAX = 10000;
  public static final int ITEM_DATA_MIN_LENGTH = 26;
  public static final int ITEM_DATA_MAX_LENGTH = 50;
  public static final int ORIGINAL_DATA_THRESHOLD = 10; // 10% chance
  public static final int BAD_CREDIT_THRESHOLD = 10; // 10% chance

  // Warehouse Constants
  public static final double WAREHOUSE_INITIAL_YTD = 300000;
  public static final int WAREHOUSE_TAX_MAX = 2000;

  // District Constants
  public static final double DISTRICT_INITIAL_YTD = 30000;
  public static final int DISTRICT_TAX_MAX = 2000;

  // Customer Constants
  public static final double CUSTOMER_CREDIT_LIMIT = 50000;
  public static final double CUSTOMER_INITIAL_BALANCE = -10;
  public static final double CUSTOMER_INITIAL_YTD_PAYMENT = 10;
  public static final int CUSTOMER_INITIAL_PAYMENT_CNT = 1;
  public static final int CUSTOMER_INITIAL_DELIVERY_CNT = 0;
  public static final int CUSTOMER_DISCOUNT_MAX = 5000;

  // Stock Constants
  public static final int STOCK_QUANTITY_MIN = 10;
  public static final int STOCK_QUANTITY_MAX = 100;

  // Order Constants
  public static final int ORDER_LINE_COUNT_MIN = 5;
  public static final int ORDER_LINE_COUNT_MAX = 15;
  public static final int ORDER_LINE_QUANTITY = 5;
  public static final int CARRIER_ID_MIN = 1;
  public static final int CARRIER_ID_MAX = 10;

  // History Constants
  public static final double HISTORY_AMOUNT = 10;
  public static final int HISTORY_DATA_MIN_LENGTH = 10;
  public static final int HISTORY_DATA_MAX_LENGTH = 24;
}
