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

package org.apache.druid.segment.loading;

public interface VirtualStorageLocationStats
{
  /**
   * Current number of bytes stored which are managed as virtual storage at the time this measurement collection was
   * created.
   */
  long getUsedBytes();

  /**
   * Number of active holds on cache entries, indicating active usage at the time this measurement collection was
   * created.
   */
  long getHoldCount();

  /**
   * Number of bytes from active holds on cache entries, indicating active usage at the time this measurement
   * collection was created.
   */
  long getHoldBytes();

  /**
   * Number of operations for which an entry was already present during the measurement period
   */
  long getHitCount();

  /**
   * Number of bytes used by operations on entries which were already present during the measurement period
   */
  long getHitBytes();

  /**
   * Number of operations for which an entry was missing and a load was started (space reserved) during the measurement
   * period. This is incremented when the load begins, regardless of whether it ultimately completes.
   */
  long getLoadBeginCount();

  /**
   * Number of bytes for entries missing from the cache for which a load was started (space reserved) during the
   * measurement period. This is incremented when the load begins, regardless of whether it ultimately completes.
   */
  long getLoadBeginBytes();

  /**
   * Number of cache entries that finished loading (segment downloaded, deserialized, and made queryable) during the
   * measurement period. This may be lower than {@link #getLoadBeginCount()} when mounts fail or are aborted.
   */
  long getLoadCount();

  /**
   * Number of bytes for cache entries that finished loading during the measurement period.
   */
  long getLoadBytes();

  /**
   * Number of cache entries removed during the measurement period
   */
  long getEvictionCount();

  /**
   * Number of bytes removed from the cache during the measurement period
   */
  long getEvictionBytes();

  /**
   * Number of operations which could not be loaded due to insufficient space during the measurement period
   */
  long getRejectCount();

  /**
   * Whether any stats are nonzero.
   */
  default boolean hasStats()
  {
    return getUsedBytes() != 0
           || getHoldCount() != 0
           || getHoldBytes() != 0
           || getHitCount() != 0
           || getHitBytes() != 0
           || getLoadBeginCount() != 0
           || getLoadBeginBytes() != 0
           || getLoadCount() != 0
           || getLoadBytes() != 0
           || getEvictionCount() != 0
           || getEvictionBytes() != 0
           || getRejectCount() != 0;
  }
}
