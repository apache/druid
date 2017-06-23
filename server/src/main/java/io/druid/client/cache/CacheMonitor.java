/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client.cache;

import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;
import io.druid.java.util.common.StringUtils;

public class CacheMonitor extends AbstractMonitor
{
  // package private for tests
  volatile Cache cache;

  private volatile CacheStats prevCacheStats = null;

  public CacheMonitor()
  {
  }

  public CacheMonitor(
      Cache cache
  )
  {
    this.cache = cache;
  }

  // make it possible to enable CacheMonitor even if cache is not bound
  // (e.g. some index tasks may have a cache, others may not)
  @Inject(optional = true)
  public void setCache(Cache cache)
  {
    this.cache = cache;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    if (cache != null) {
      final CacheStats currCacheStats = cache.getStats();
      final CacheStats deltaCacheStats = currCacheStats.delta(prevCacheStats);

      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
      emitStats(emitter, "query/cache/delta", deltaCacheStats, builder);
      emitStats(emitter, "query/cache/total", currCacheStats, builder);

      prevCacheStats = currCacheStats;

      // Any custom cache statistics that need monitoring
      cache.doMonitor(emitter);
    }
    return true;
  }

  private void emitStats(
      ServiceEmitter emitter,
      final String metricPrefix,
      CacheStats cacheStats,
      ServiceMetricEvent.Builder builder
  )
  {
    if (cache != null) {
      emitter.emit(builder.build(StringUtils.format("%s/numEntries", metricPrefix), cacheStats.getNumEntries()));
      emitter.emit(builder.build(StringUtils.format("%s/sizeBytes", metricPrefix), cacheStats.getSizeInBytes()));
      emitter.emit(builder.build(StringUtils.format("%s/hits", metricPrefix), cacheStats.getNumHits()));
      emitter.emit(builder.build(StringUtils.format("%s/misses", metricPrefix), cacheStats.getNumMisses()));
      emitter.emit(builder.build(StringUtils.format("%s/evictions", metricPrefix), cacheStats.getNumEvictions()));
      emitter.emit(builder.build(StringUtils.format("%s/hitRate", metricPrefix), cacheStats.hitRate()));
      emitter.emit(builder.build(StringUtils.format("%s/averageBytes", metricPrefix), cacheStats.averageBytes()));
      emitter.emit(builder.build(StringUtils.format("%s/timeouts", metricPrefix), cacheStats.getNumTimeouts()));
      emitter.emit(builder.build(StringUtils.format("%s/errors", metricPrefix), cacheStats.getNumErrors()));
    }
  }
}
