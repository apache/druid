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

import org.apache.druid.metadata.segment.DatasourceSegmentMetadataReader;
import org.apache.druid.metadata.segment.DatasourceSegmentMetadataWriter;

/**
 * TODO:
 * -[ ] Write unit test for DatasourceSegmentCache and SqlSegmentsMetadataCache
 *
 * -[ ] Wire up cache in OverlordCompactionScheduler and SqlSegmentsMetadataManager,
 * otherwise we will end up having two copies of the segment timeline and stuff
 * The timeline inside the cache can replace the SegmentTimeline of SqlSegmentsMetadataManager
 * -[ ] Add transaction API to return timeline and/or timeline holders
 * -[ ] Think about race conditions in the cache - leadership changes, multiple concurrent transactions
 * -[ ] Write unit tests
 * -[ ] Write integration tests
 * -[ ] Write a benchmark
 */
public interface SegmentsMetadataCache
{
  void start();

  void stop();

  /**
   * @return true if the cache is enabled and ready for reading and writing.
   */
  boolean isReady();

  DataSource getDatasource(String dataSource);

  /**
   * Cache containing segment metadata of a single datasource.
   */
  interface DataSource extends DatasourceSegmentMetadataWriter, DatasourceSegmentMetadataReader
  {
    /**
     * Performs a thread-safe read action on the cache.
     */
    <T> T read(Action<T> action) throws Exception;

    /**
     * Performs a thread-safe write action on the cache.
     */
    <T> T write(Action<T> action) throws Exception;
  }

  /**
   * Represents a read or write action performed on the cache within required
   * locks.
   */
  @FunctionalInterface
  interface Action<T>
  {
    T perform() throws Exception;
  }

}
