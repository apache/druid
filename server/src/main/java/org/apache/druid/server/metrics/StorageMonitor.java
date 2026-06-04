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

import com.google.common.base.Supplier;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationStats;
import org.apache.druid.segment.loading.VirtualStorageLocationStats;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Monitor to emit stats from {@link StorageLocation}.
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

  /**
   * Total number of bytes reserved by strongly-held objects in the storage location. Includes reservations that are not
   * yet loaded.
   */
  public static final String USED_BYTES = "storage/used/bytes";

  /**
   * Number of strongly-held objects whose load was started during the measurement period. Incremented when space is
   * reserved, before the object has been downloaded.
   */
  public static final String LOAD_BEGIN_COUNT = "storage/load/begin/count";

  /**
   * Total bytes of strongly-held objects whose load was started during the measurement period.
   */
  public static final String LOAD_BEGIN_BYTES = "storage/load/begin/bytes";

  /**
   * Number of strongly-held objects whose load completed during the measurement period. Incremented after the object
   * has been downloaded and is usable.
   */
  public static final String LOAD_COUNT = "storage/load/count";

  /**
   * Total bytes of strongly-held objects whose load completed during the measurement period.
   */
  public static final String LOAD_BYTES = "storage/load/bytes";

  /**
   * Number of strongly-held objects dropped from the storage location during the measurement period.
   */
  public static final String DROP_COUNT = "storage/drop/count";

  /**
   * Total bytes of strongly-held objects dropped from the storage location during the measurement period.
   */
  public static final String DROP_BYTES = "storage/drop/bytes";

  /**
   * Total number of bytes reserved by weakly-held objects in virtual storage. Includes reservations that are not yet
   * loaded.
   */
  public static final String VSF_USED_BYTES = "storage/virtual/used/bytes";

  /**
   * Number of active holds on weakly-held objects, indicating objects currently in use.
   */
  public static final String VSF_HOLD_COUNT = "storage/virtual/hold/count";

  /**
   * Total bytes from active holds on weakly-held objects.
   */
  public static final String VSF_HOLD_BYTES = "storage/virtual/hold/bytes";

  /**
   * Number of acquire operations during the measurement period that found an existing weakly-held entry already in
   * virtual storage.
   */
  public static final String VSF_HIT_COUNT = "storage/virtual/hit/count";

  /**
   * Total bytes from acquire operations during the measurement period that found an existing weakly-held entry already
   * in virtual storage.
   */
  public static final String VSF_HIT_BYTES = "storage/virtual/hit/bytes";

  /**
   * Number of weakly-held objects whose load was started during the measurement period. Incremented when space is
   * reserved, before the object has been downloaded.
   */
  public static final String VSF_LOAD_BEGIN_COUNT = "storage/virtual/load/begin/count";

  /**
   * Total bytes of weakly-held objects whose load was started during the measurement period.
   */
  public static final String VSF_LOAD_BEGIN_BYTES = "storage/virtual/load/begin/bytes";

  /**
   * Number of weakly-held objects whose load completed during the measurement period. Incremented after the object has
   * been downloaded and is usable.
   */
  public static final String VSF_LOAD_COUNT = "storage/virtual/load/count";

  /**
   * Total bytes of weakly-held objects whose load completed during the measurement period.
   */
  public static final String VSF_LOAD_BYTES = "storage/virtual/load/bytes";

  /**
   * Number of weakly-held objects evicted from virtual storage during the measurement period.
   */
  public static final String VSF_EVICT_COUNT = "storage/virtual/evict/count";

  /**
   * Total bytes of weakly-held objects evicted from virtual storage during the measurement period.
   */
  public static final String VSF_EVICT_BYTES = "storage/virtual/evict/bytes";

  /**
   * Number of acquire operations during the measurement period that could not load a weakly-held object due to
   * insufficient space in virtual storage.
   */
  public static final String VSF_REJECT_COUNT = "storage/virtual/reject/count";

  private final List<StorageLocation> locations;
  private final Supplier<ServiceMetricEvent.Builder> builderSupplier;

  public StorageMonitor(
      List<StorageLocation> locations,
      @Nullable Supplier<ServiceMetricEvent.Builder> builderSupplier
  )
  {
    this.locations = locations;
    this.builderSupplier = builderSupplier == null ? ServiceMetricEvent.Builder::new : builderSupplier;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (StorageLocation location : locations) {
      final String label = location.getPath().toString();

      final StorageLocationStats staticStats = location.resetStaticStats();
      final ServiceMetricEvent.Builder builder = builderSupplier.get().setDimension(LOCATION_DIMENSION, label);
      if (staticStats.hasStats()) {
        emitter.emit(builder.setMetric(USED_BYTES, staticStats.getUsedBytes()));
        emitter.emit(builder.setMetric(LOAD_COUNT, staticStats.getLoadCount()));
        emitter.emit(builder.setMetric(LOAD_BYTES, staticStats.getLoadBytes()));
        emitter.emit(builder.setMetric(LOAD_BEGIN_COUNT, staticStats.getLoadBeginCount()));
        emitter.emit(builder.setMetric(LOAD_BEGIN_BYTES, staticStats.getLoadBeginBytes()));
        emitter.emit(builder.setMetric(DROP_COUNT, staticStats.getDropCount()));
        emitter.emit(builder.setMetric(DROP_BYTES, staticStats.getDropBytes()));
      }

      final VirtualStorageLocationStats weakStats = location.resetWeakStats();
      if (weakStats.hasStats()) {
        emitter.emit(builder.setMetric(VSF_USED_BYTES, weakStats.getUsedBytes()));
        emitter.emit(builder.setMetric(VSF_HOLD_COUNT, weakStats.getHoldCount()));
        emitter.emit(builder.setMetric(VSF_HOLD_BYTES, weakStats.getHoldBytes()));
        emitter.emit(builder.setMetric(VSF_HIT_COUNT, weakStats.getHitCount()));
        emitter.emit(builder.setMetric(VSF_HIT_BYTES, weakStats.getHitBytes()));
        emitter.emit(builder.setMetric(VSF_LOAD_BEGIN_COUNT, weakStats.getLoadBeginCount()));
        emitter.emit(builder.setMetric(VSF_LOAD_BEGIN_BYTES, weakStats.getLoadBeginBytes()));
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
