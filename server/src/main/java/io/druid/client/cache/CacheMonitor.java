/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.client.cache;

import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;

/**
 */
public class CacheMonitor extends AbstractMonitor
{
  private final Cache cache;

  private volatile CacheStats prevCacheStats = null;

  @Inject
  public CacheMonitor(
      Cache cache
  )
  {
    this.cache = cache;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final CacheStats currCacheStats = cache.getStats();
    final CacheStats deltaCacheStats = currCacheStats.delta(prevCacheStats);

    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    emitStats(emitter, "cache/delta", deltaCacheStats, builder);
    emitStats(emitter, "cache/total", currCacheStats, builder);

    prevCacheStats = currCacheStats;
    return true;
  }

  private void emitStats(
      ServiceEmitter emitter,
      final String metricPrefix,
      CacheStats cacheStats,
      ServiceMetricEvent.Builder builder
  )
  {
    emitter.emit(builder.build(String.format("%s/numEntries", metricPrefix), cacheStats.getNumEntries()));
    emitter.emit(builder.build(String.format("%s/sizeBytes", metricPrefix), cacheStats.getSizeInBytes()));
    emitter.emit(builder.build(String.format("%s/hits", metricPrefix), cacheStats.getNumHits()));
    emitter.emit(builder.build(String.format("%s/misses", metricPrefix), cacheStats.getNumMisses()));
    emitter.emit(builder.build(String.format("%s/evictions", metricPrefix), cacheStats.getNumEvictions()));
    emitter.emit(builder.build(String.format("%s/hitRate", metricPrefix), cacheStats.hitRate()));
    emitter.emit(builder.build(String.format("%s/averageBytes", metricPrefix), cacheStats.averageBytes()));
    emitter.emit(builder.build(String.format("%s/timeouts", metricPrefix), cacheStats.getNumTimeouts()));
    emitter.emit(builder.build(String.format("%s/errors", metricPrefix), cacheStats.getNumErrors()));
  }
}
