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
   * Returns the cache for the given datasource.
   */
  DatasourceSegmentCache getDatasource(String dataSource);

}
