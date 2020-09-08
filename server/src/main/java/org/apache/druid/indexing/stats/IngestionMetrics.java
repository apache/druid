/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.stats;

/**
 * A collection of metrics used in all ingestion types. It includes all metrics for
 * {@link org.apache.druid.segment.realtime.appenderator.Appenderator}.
 *
 * Implementations must be thread-safe as multiple threads can call any methods at the same time.
 */
public interface IngestionMetrics
{
  default void incrementRowsProcessed()
  {
    incrementRowsProcessed(1);
  }

  void incrementRowsProcessed(long n);

  default void incrementRowsProcessedWithErrors()
  {
    incrementRowsProcessedWithErrors(1);
  }

  void incrementRowsProcessedWithErrors(long n);

  default void incrementRowsThrownAway()
  {
    incrementRowsThrownAway(1);
  }

  void incrementRowsThrownAway(long n);

  default void incrementRowsUnparseable()
  {
    incrementRowsUnparseable(1);
  }

  void incrementRowsUnparseable(long n);

  void incrementRowsOut(long numRows);

  default void incrementNumPersists()
  {
    incrementNumPersists(1);
  }

  void incrementNumPersists(long n);

  void incrementPersistTimeMillis(long millis);

  void incrementPersistBackPressureMillis(long millis);

  default void incrementFailedPersists()
  {
    incrementFailedPersists(1);
  }

  void incrementFailedPersists(long n);

  void incrementPersistCpuTime(long persistTime);

  void incrementMergeTimeMillis(long millis);

  void incrementMergeCpuTime(long mergeTime);

  void reportMessageMaxTimestamp(long messageMaxTimestamp);

  /**
   * Returns a snapshot of the current metrics.
   *
   * Note on concurrency: the returned snapshot doesn't have to be very accurate. That means, the happens-before
   * relationship exists when any other method is called before than snapshot(), but not in the opposite way.
   */
  IngestionMetricsSnapshot snapshot();
}
