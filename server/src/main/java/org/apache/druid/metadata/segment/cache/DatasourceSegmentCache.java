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
 * Cache containing segment metadata of a single datasource.
 */
public interface DatasourceSegmentCache extends DatasourceSegmentMetadataWriter, DatasourceSegmentMetadataReader
{
  /**
   * Performs a thread-safe read action on the cache.
   * Read actions can be concurrent with other reads but are mutually exclusive
   * from other write actions.
   */
  <T> T read(Action<T> action) throws Exception;

  /**
   * Performs a thread-safe write action on the cache.
   * Write actions are mutually exclusive from other writes or reads.
   */
  <T> T write(Action<T> action) throws Exception;

  /**
   * Represents a thread-safe read or write action performed on the cache within
   * required locks.
   */
  @FunctionalInterface
  interface Action<T>
  {
    T perform() throws Exception;
  }
}
