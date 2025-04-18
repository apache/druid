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
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.error.InvalidInput;

/**
 * Cache for metadata of pending segments and used segments maintained by
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
   * Waits until the cache finishes the next sync with the metadata store or
   * until the timeout elapses, whichever is sooner.
   */
  void awaitNextSync(long timeoutMillis);

  /**
   * @return Latest snapshot of the datasources with all their used segments.
   */
  DataSourcesSnapshot getDataSourcesSnapshot();

  /**
   * Performs a thread-safe read action on the cache for the given datasource.
   * Read actions can be concurrent with other reads but are mutually exclusive
   * from other write actions on the same datasource.
   */
  <T> T readCacheForDataSource(String dataSource, Action<T> readAction);

  /**
   * Performs a thread-safe write action on the cache for the given datasource.
   * Write actions are mutually exclusive from other writes or reads on the same
   * datasource.
   */
  <T> T writeCacheForDataSource(String dataSource, Action<T> writeAction);

  /**
   * Represents a thread-safe read or write action performed on the cache within
   * required locks.
   */
  @FunctionalInterface
  interface Action<T>
  {
    T perform(DatasourceSegmentCache dataSourceCache) throws Exception;
  }

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
    ALWAYS("always"),

    /**
     * Cache is disabled.
     */
    NEVER("never"),

    /**
     * Read from the cache only if it is already synced with the metadata store.
     * Does not block service start-up or transactions. Writes may still go to
     * cache to reduce sync times.
     */
    IF_SYNCED("ifSynced");

    private final String name;

    UsageMode(String name)
    {
      this.name = name;
    }

    @JsonCreator
    public static UsageMode fromString(String value)
    {
      for (UsageMode mode : values()) {
        if (mode.toString().equals(value)) {
          return mode;
        }
      }

      throw InvalidInput.exception("No such cache usage mode[%s]", value);
    }

    @Override
    public String toString()
    {
      return name;
    }
  }
}
