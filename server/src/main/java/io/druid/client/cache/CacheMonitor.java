/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
