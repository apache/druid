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

package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.StorageLocationStats;
import org.apache.druid.segment.loading.StorageStats;
import org.apache.druid.segment.loading.VirtualStorageLocationStats;

import java.util.Map;

/**
 * Monitor to emit output of {@link SegmentCacheManager#getStorageStats()}
 */
@LoadScope(roles = {
    NodeRole.BROKER_JSON_NAME,
    NodeRole.HISTORICAL_JSON_NAME,
    NodeRole.INDEXER_JSON_NAME,
    NodeRole.PEON_JSON_NAME
})
public class StorageMonitor extends AbstractMonitor
{
  public static final String LOCATION_DIMENSION = "location";
  public static final String USED_BYTES = "storage/used/bytes";
  public static final String LOAD_COUNT = "storage/load/count";
  public static final String LOAD_BYTES = "storage/load/bytes";
  public static final String DROP_COUNT = "storage/drop/count";
  public static final String DROP_BYTES = "storage/drop/bytes";
  public static final String VSF_USED_BYTES = "storage/virtual/used/bytes";
  public static final String VSF_HIT_COUNT = "storage/virtual/hit/count";
  public static final String VSF_LOAD_COUNT = "storage/virtual/load/count";
  public static final String VSF_LOAD_BYTES = "storage/virtual/load/bytes";
  public static final String VSF_EVICT_COUNT = "storage/virtual/evict/count";
  public static final String VSF_EVICT_BYTES = "storage/virtual/evict/bytes";
  public static final String VSF_REJECT_COUNT = "storage/virtual/reject/count";

  private final SegmentCacheManager cacheManager;

  @Inject
  public StorageMonitor(
      SegmentCacheManager cacheManager
  )
  {
    this.cacheManager = cacheManager;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final StorageStats stats = cacheManager.getStorageStats();

    if (stats != null) {
      for (Map.Entry<String, StorageLocationStats> location : stats.getLocationStats().entrySet()) {
        final StorageLocationStats staticStats = location.getValue();
        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
            .setDimension(LOCATION_DIMENSION, location.getKey());
        emitter.emit(builder.setMetric(USED_BYTES, staticStats.getUsedBytes()));
        emitter.emit(builder.setMetric(LOAD_COUNT, staticStats.getLoadCount()));
        emitter.emit(builder.setMetric(LOAD_BYTES, staticStats.getLoadBytes()));
        emitter.emit(builder.setMetric(DROP_COUNT, staticStats.getDropCount()));
        emitter.emit(builder.setMetric(DROP_BYTES, staticStats.getDropBytes()));
      }

      for (Map.Entry<String, VirtualStorageLocationStats> location : stats.getVirtualLocationStats().entrySet()) {
        final VirtualStorageLocationStats weakStats = location.getValue();
        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder().setDimension(
            LOCATION_DIMENSION,
            location.getKey()
        );
        emitter.emit(builder.setMetric(VSF_USED_BYTES, weakStats.getUsedBytes()));
        emitter.emit(builder.setMetric(VSF_HIT_COUNT, weakStats.getHitCount()));
        emitter.emit(builder.setMetric(VSF_LOAD_COUNT, weakStats.getLoadCount()));
        emitter.emit(builder.setMetric(VSF_LOAD_BYTES, weakStats.getLoadBytes()));
        emitter.emit(builder.setMetric(VSF_EVICT_COUNT, weakStats.getEvictionCount()));
        emitter.emit(builder.setMetric(VSF_EVICT_BYTES, weakStats.getEvictionBytes()));
        emitter.emit(builder.setMetric(VSF_REJECT_COUNT, weakStats.getRejectCount()));
      }
    }
    return true;
  }
}
