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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.codec.digest.DigestUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CaffeineCache implements io.druid.client.cache.Cache
{
  private static final Logger log = new Logger(CaffeineCache.class);
  private final Cache<String, byte[]> cache;
  private final AtomicReference<com.github.benmanes.caffeine.cache.stats.CacheStats> priorStats = new AtomicReference<>(
      null);


  public static CaffeineCache create(final CaffeineCacheConfig config)
  {
    Caffeine<Object, Object> builder = Caffeine.newBuilder().recordStats();
    if (config.getExpiration() >= 0) {
      builder = builder
          .expireAfterAccess(config.getExpiration(), TimeUnit.MILLISECONDS);
    }
    if (config.getMaxSize() >= 0) {
      builder = builder
          .maximumSize(config.getMaxSize());
    }
    return new CaffeineCache(builder.<String, byte[]>build());
  }

  public CaffeineCache(final Cache<String, byte[]> cache)
  {
    this.cache = cache;
  }

  @Override
  public byte[] get(NamedKey key)
  {
    final String itemKey = computeKeyHash(key);
    return deserialize(cache.getIfPresent(itemKey));
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    final String itemKey = computeKeyHash(key);
    cache.put(itemKey, serialize(value));
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    final Map<String, NamedKey> keyLookup = Maps.uniqueIndex(
        keys,
        new Function<NamedKey, String>()
        {
          @Override
          public String apply(
              @Nullable NamedKey input
          )
          {
            return computeKeyHash(input);
          }
        }
    );

    // Sometimes broker passes empty keys list to getBulk()
    if (keyLookup.size() == 0) {
      return ImmutableMap.of();
    }

    final Map<NamedKey, byte[]> results = Maps.newHashMap();
    final Map<String, byte[]> cachedVals = cache.getAllPresent(Iterables.transform(
        keys,
        new Function<NamedKey, String>()
        {
          @Override
          public String apply(
              NamedKey input
          )
          {
            return computeKeyHash(input);
          }
        }
    ));

    // Hash join
    for (String key : cachedVals.keySet()) {
      final byte[] val = deserialize(cachedVals.get(key));
      if (val != null) {
        results.put(keyLookup.get(key), val);
      }
    }
    return results;
  }

  // This is completely racy with put. Any values missed should be evicted later anyways. So no worries.
  @Override
  public void close(String namespace)
  {
    final String keyPrefix = computeNamespaceHash(namespace) + ":";
    for (String key : cache.asMap().keySet()) {
      if (key.startsWith(keyPrefix)) {
        cache.invalidate(key);
      }
    }
  }

  @Override
  public CacheStats getStats()
  {
    final com.github.benmanes.caffeine.cache.stats.CacheStats stats = cache.stats();
    return new CacheStats(
        stats.hitCount(),
        stats.missCount(),
        stats.loadSuccessCount() - stats.evictionCount(),
        cache.estimatedSize(),
        stats.evictionCount(),
        0,
        stats.loadFailureCount()
    );
  }

  @Override
  public boolean isLocal()
  {
    return true;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    final com.github.benmanes.caffeine.cache.stats.CacheStats oldStats = priorStats.get();
    final com.github.benmanes.caffeine.cache.stats.CacheStats newStats = cache.stats();
    final com.github.benmanes.caffeine.cache.stats.CacheStats deltaStats;
    if (oldStats == null) {
      deltaStats = newStats;
    } else {
      deltaStats = newStats.minus(oldStats);
    }
    final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    emitter.emit(builder.build("query/cache/caffeine/delta/requests", deltaStats.requestCount()));
    emitter.emit(builder.build("query/cache/caffeine/total/requests", newStats.requestCount()));
    emitter.emit(builder.build("query/cache/caffeine/delta/loadTime", deltaStats.totalLoadTime()));
    emitter.emit(builder.build("query/cache/caffeine/total/loadTime", newStats.totalLoadTime()));
    if (!priorStats.compareAndSet(oldStats, newStats)) {
      // ISE for stack trace
      log.warn(
          new IllegalStateException("Multiple monitors"),
          "Multiple monitors on the same cache causing race conditions and unreliable stats reporting"
      );
    }
  }

  private final LZ4Factory factory = LZ4Factory.fastestInstance();

  private byte[] deserialize(byte[] bytes)
  {
    if (bytes == null) {
      return null;
    }
    final int decompressedLen = ByteBuffer.wrap(bytes).getInt();
    final byte[] out = new byte[decompressedLen];
    final int bytesRead = factory.fastDecompressor().decompress(bytes, Ints.BYTES, out, 0, out.length);
    if (bytesRead != bytes.length - Ints.BYTES) {
      if (log.isDebugEnabled()) {
        log.debug("Bytes read [%s] does not equal expected bytes read [%s]", bytesRead, bytes.length - Ints.BYTES);
      }
    }
    return out;
  }

  private byte[] serialize(byte[] value)
  {
    final LZ4Compressor compressor = factory.fastCompressor();
    final int len = compressor.maxCompressedLength(value.length);
    final byte[] out = new byte[len];
    final int compressedSize = compressor.compress(value, 0, value.length, out, 0);
    return ByteBuffer.allocate(compressedSize + Ints.BYTES)
                     .putInt(value.length)
                     .put(out, 0, compressedSize)
                     .array();
  }

  public static String computeKeyHash(final NamedKey key)
  {
    return String.format("%s:%s", computeNamespaceHash(key.namespace), DigestUtils.sha1Hex(key.key));
  }

  // So people can't do weird things with namespace strings
  public static String computeNamespaceHash(final String namespace)
  {
    return DigestUtils.sha1Hex(namespace);
  }
}
