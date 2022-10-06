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

package org.apache.druid.msq.statistics;

import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.RowKey;

public interface KeyCollector<CollectorType extends KeyCollector<CollectorType>>
{
  /**
   * Add a key with a certain weight to this collector.
   *
   * See {@link ClusterByStatisticsCollector#add} for the meaning of "weight".
   */
  void add(RowKey key, long weight);

  /**
   * Fold another collector into this one.
   */
  void addAll(CollectorType other);

  /**
   * Returns whether this collector is empty.
   */
  boolean isEmpty();

  /**
   * Returns an estimate of the amount of total weight currently tracked by this collector. This may change over
   * time as more keys are added.
   */
  long estimatedTotalWeight();

  /**
   * Returns an estimate of the number of keys currently retained by this collector. This may change over time as
   * more keys are added.
   */
  int estimatedRetainedKeys();

  /**
   * Returns an estimate of the number of bytes currently retained by this collector. This may change over time as
   * more keys are added.
   */
  double estimatedRetainedBytes();

  /**
   * Downsample this collector, dropping about half of the keys that are currently retained. Returns true if
   * the collector was downsampled, or if it is already retaining zero or one keys. Returns false if the collector is
   * retaining more than one key, yet cannot be downsampled any further.
   */
  boolean downSample();

  /**
   * Returns the minimum key encountered by this collector so far, if any have been encountered.
   *
   * @throws java.util.NoSuchElementException if the collector is empty; i.e. if {@link #isEmpty()} is true.
   */
  RowKey minKey();

  /**
   * Generates key ranges, targeting a particular row weight per range.
   *
   * @param targetWeight row weight per partition. The actual amount of row weight per range may be higher
   *                     or lower than the provided target.
   */
  ClusterByPartitions generatePartitionsWithTargetWeight(long targetWeight);
}
