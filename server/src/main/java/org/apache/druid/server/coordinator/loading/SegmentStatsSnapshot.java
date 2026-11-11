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

package org.apache.druid.server.coordinator.loading;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2LongMap;

import java.util.Collections;
import java.util.Map;

/**
 * Holder for the three segment-stat views computed together by
 * {@link SegmentReplicationStatus#computeSegmentStats}.
 */
public class SegmentStatsSnapshot
{
  private static final SegmentStatsSnapshot EMPTY = new SegmentStatsSnapshot(
      Object2IntMaps.emptyMap(),
      Collections.emptyMap(),
      Object2IntMaps.emptyMap()
  );

  private final Object2IntMap<String> datasourceToUnavailableCount;
  private final Map<String, Object2LongMap<String>> tierToDatasourceToUnderReplicatedCount;
  private final Object2IntMap<String> datasourceToDeepStorageOnlyCount;

  SegmentStatsSnapshot(
      Object2IntMap<String> datasourceToUnavailableCount,
      Map<String, Object2LongMap<String>> tierToDatasourceToUnderReplicatedCount,
      Object2IntMap<String> datasourceToDeepStorageOnlyCount
  )
  {
    this.datasourceToUnavailableCount = datasourceToUnavailableCount;
    this.tierToDatasourceToUnderReplicatedCount = tierToDatasourceToUnderReplicatedCount;
    this.datasourceToDeepStorageOnlyCount = datasourceToDeepStorageOnlyCount;
  }

  /**
   * Returns an immutable, empty snapshot, used when no replication status is available.
   */
  public static SegmentStatsSnapshot empty()
  {
    return EMPTY;
  }

  public Object2IntMap<String> getDatasourceToUnavailableCount()
  {
    return datasourceToUnavailableCount;
  }

  public Map<String, Object2LongMap<String>> getTierToDatasourceToUnderReplicatedCount()
  {
    return tierToDatasourceToUnderReplicatedCount;
  }

  public Object2IntMap<String> getDatasourceToDeepStorageOnlyCount()
  {
    return datasourceToDeepStorageOnlyCount;
  }
}
