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

package com.oltpbenchmark.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public abstract class TimeUtil {

  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
  public static final SimpleDateFormat DATE_FORMAT_14 = new SimpleDateFormat("yyyyMMddHHmmss");

  /**
   * TODO(djellel)
   *
   * @return
   */
  public static String getCurrentTimeString14() {
    return TimeUtil.DATE_FORMAT_14.format(new java.util.Date());
  }

  /**
   * TODO(djellel)
   *
   * @return
   */
  public static String getCurrentTimeString() {
    return TimeUtil.DATE_FORMAT.format(new java.util.Date());
  }

  /** Get a timestamp of the current time */
  public static Timestamp getCurrentTime() {
    return new Timestamp(System.currentTimeMillis());
  }

  /**
   * Calculate exponential backoff delay with jitter!
   *
   * <p>NOTE: Added to time util class to avoid additional class just for one method.
   *
   * @param attempts
   * @return Exponential Delay
   */
  public static long calExpDelay(int attempts) {
    long baseDelay = 1000; // Initial delay in milliseconds
    double jitterFactor = 1.0; // Jitter factor (between 0 and 1)

    long delay = (long) (baseDelay * Math.pow(2, attempts));
    delay = (long) (delay * (1 + jitterFactor * Math.random()));
    delay = Math.min(delay, 4000);

    return delay;
  }

  public static long getTimeDiffInMicro(long startNano, long endNano) {
    return (endNano - startNano) / 1000;
  }
}
