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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;

/**
 * Generic batch processor for handling batch inserts with configurable batch size. Manages
 * accumulation of items and automatic flushing when batch size is reached.
 *
 * @param <T> The type of items to be batch processed
 */
@Slf4j
public class BatchProcessor<T> {

  private final int batchSize;
  private final List<T> batch;
  private final BiConsumer<PreparedStatement, T> statementSetter;

  /**
   * Creates a new batch processor.
   *
   * @param batchSize The size at which to automatically flush the batch
   * @param statementSetter Function to set parameters on the prepared statement for each item
   * @param metricName Name for metrics tracking
   */
  public BatchProcessor(int batchSize, BiConsumer<PreparedStatement, T> statementSetter) {
    this.batchSize = batchSize;
    this.batch = new ArrayList<>(batchSize);
    this.statementSetter = statementSetter;
  }

  /**
   * Adds an item to the batch. Does NOT auto-flush. Caller is responsible for calling flush() when
   * ready.
   *
   * @param item The item to add
   */
  public void add(T item) {
    batch.add(item);
  }

  /**
   * Flushes items in the batch, processing in chunks of batchSize. This handles cases where the
   * batch may have accumulated more items than batchSize. Only removes items from the batch after
   * successful execution.
   *
   * @param statement The prepared statement to use for execution
   * @throws SQLException if database operation fails
   */
  public void flush(PreparedStatement statement) throws SQLException {
    while (!batch.isEmpty()) {
      int itemsToFlush = Math.min(batch.size(), batchSize);
      List<T> currentBatch = batch.subList(0, itemsToFlush);

      // Execute this chunk
      for (T item : currentBatch) {
        statementSetter.accept(statement, item);
        statement.addBatch();
      }

      statement.executeBatch();
      statement.clearBatch();

      log.debug("Executed batch of {} items", itemsToFlush);

      // Only remove if successful
      currentBatch.clear(); // Removes from original batch
    }
  }

  /** Returns the current number of items in the batch. */
  public int size() {
    return batch.size();
  }

  /** Returns true if the batch has reached the configured batch size and should be flushed. */
  public boolean shouldFlush() {
    return batch.size() >= batchSize;
  }

  /** Clears the batch without executing. */
  public void clear() {
    batch.clear();
  }
}
