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

import org.apache.druid.guice.annotations.UnstableApi;

import java.util.Map;

/**
 * Collection of {@link StorageLocationStats} and {@link VirtualStorageLocationStats} for all storage locations within
 * a {@link SegmentCacheManager} so that {@link SegmentCacheManager#getStorageStats()} can be used by
 * {@link org.apache.druid.server.metrics.StorageMonitor} to track segment cache activity.
 * <p>
 * Note that the stats are not tied explicitly to the {@link StorageLocation} implementation used by
 * {@link SegmentLocalCacheManager}, but it does implement this stuff.
 */
@UnstableApi
public class StorageStats
{
  private final Map<String, StorageLocationStats> stats;
  private final Map<String, VirtualStorageLocationStats> virtualStats;

  public StorageStats(
      final Map<String, StorageLocationStats> stats,
      final Map<String, VirtualStorageLocationStats> virtualStats
  )
  {
    this.stats = stats;
    this.virtualStats = virtualStats;
  }

  /**
   * Map of location label (such as file path) to {@link StorageLocationStats}
   */
  public Map<String, StorageLocationStats> getLocationStats()
  {
    return stats;
  }

  /**
   * Map of location label (such as file path) to {@link VirtualStorageLocationStats}
   */
  public Map<String, VirtualStorageLocationStats> getVirtualLocationStats()
  {
    return virtualStats;
  }
}
