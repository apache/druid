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
 * -[x] Finish polling of pending segments properly
 * -[x] Implement rollback and commit for cached transaction
 * -[x] Acquire read/write lock on datasource cache when transaction starts.
 * -[x] Add different factory methods to create read vs write transaction
 * -[x] Write a basic unit test to verify that things are working as expected
 * -[ ] Write unit test for DatasourceSegmentCache and SqlSegmentsMetadataCache
 * -
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

  boolean isReady();

  DataSource getDatasource(String dataSource);

  interface DataSource extends DatasourceSegmentMetadataWriter, DatasourceSegmentMetadataReader
  {
    <T> T withReadLock(Action<T> action) throws Exception;

    <T> T withWriteLock(Action<T> action) throws Exception;
  }

  interface Action<T>
  {
    T perform(DataSource datasourceCache) throws Exception;
  }

}
