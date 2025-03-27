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

package org.apache.druid.metadata.segment.cache;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Cache for metadata of pending segments and committed segments maintained by
 * the Overlord to improve performance of segment allocation and other task actions.
 * <p>
 * Not to be confused with {@link org.apache.druid.segment.metadata.AbstractSegmentMetadataCache}
 * which is used by Brokers to cache row signature, number of rows, etc. to aid
 * with Druid query performance.
 */
public interface SegmentMetadataCache
{
  /**
   * Starts the cache on service start.
   */
  void start();

  /**
   * Stops the cache on service stop.
   */
  void stop();

  /**
   * Refreshes the cache once the service is elected leader.
   */
  void becomeLeader();

  /**
   * Notifies the cache that the service has lost leadership.
   */
  void stopBeingLeader();

  /**
   * @return true if the cache is enabled
   */
  boolean isEnabled();

  /**
   * @return true if the cache is enabled and synced with the metadata store.
   * Reads can be done from the cache only if it is synced but writes can happen
   * even before the sync has finished.
   */
  boolean isSyncedForRead();

  /**
   * Returns the cache for the given datasource.
   */
  DatasourceSegmentCache getDatasource(String dataSource);

  /**
   * Cache usage modes.
   */
  enum UsageMode
  {
    /**
     * Always read from the cache. Service start-up may be blocked until cache
     * has synced with the metadata store at least once. Transactions may block
     * until cache has synced with the metadata store at least once after
     * becoming leader.
     */
    ALWAYS,

    /**
     * Cache is disabled.
     */
    NEVER,

    /**
     * Read from the cache only if it is already synced with the metadata store.
     * Does not block service start-up or transactions. Writes may still go to
     * cache to reduce sync times.
     */
    IF_SYNCED;

    @JsonCreator
    public static UsageMode fromString(String value)
    {
      return UsageMode.valueOf(value.toUpperCase());
    }
  }
}
