package com.metamx.druid.client.cache;

import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;

/**
 */
public class CacheMonitor extends AbstractMonitor
{
  private final CacheBroker cacheBroker;

  private volatile CacheStats prevCacheStats = null;

  public CacheMonitor(
      CacheBroker cacheBroker
  )
  {
    this.cacheBroker = cacheBroker;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final CacheStats currCacheStats = cacheBroker.getStats();
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
  }
}
