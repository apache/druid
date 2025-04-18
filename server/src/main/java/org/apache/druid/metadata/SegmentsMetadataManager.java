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

package org.apache.druid.metadata;

import org.apache.druid.client.DataSourcesSnapshot;

/**
 * Polls the metadata store periodically and builds a timeline of used segments
 * (and schemas if schema caching on the Coordinator is enabled).
 * <p>
 * This class is provisioned by {@link SegmentsMetadataManagerProvider} and must
 * be bound on the Coordinator/Overlord accordingly.
 */
public interface SegmentsMetadataManager
{
  /**
   * Initializes the manager when the service is being started.
   */
  void start();

  /**
   * Cleans up resources when the service is being shut down.
   */
  void stop();

  /**
   * Starts polling segments from the metadata store upon becoming leader.
   */
  void startPollingDatabasePeriodically();

  /**
   * Stops polling segments from the metadata store when leadership is lost.
   */
  void stopPollingDatabasePeriodically();

  /**
   * @return true if currently the leader and polling the metadata store.
   */
  boolean isPollingDatabasePeriodically();

  /**
   * Returns the latest snapshot containing all used segments currently cached on the
   * manager. If the latest snapshot is older than the poll period, this method
   * blocks until the snapshot is refreshed.
   */
  DataSourcesSnapshot getDataSourceSnapshot();

  /**
   * Forces the manager to poll the metadata store and update its snapshot.
   * If a poll is already in progress, a new poll is not started. This method
   * until the poll finishes.
   *
   * @return The snapshot built after finishing the poll of the metadata store
   * triggered by this method.
   */
  DataSourcesSnapshot forceUpdateAndGetSnapshot();

  /**
   * Populates used_status_last_updated column in the segments table iteratively until there are no segments with a NULL
   * value for that column.
   */
  void populateUsedFlagLastUpdatedAsync();

  void stopAsyncUsedFlagLastUpdatedUpdate();
}
