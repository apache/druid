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

package org.apache.druid.client.cache;

import org.apache.druid.com.google.common.collect.Lists;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import redis.clients.jedis.exceptions.JedisException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractRedisCache implements Cache
{
  private static final Logger log = new Logger(AbstractRedisCache.class);

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);
  private final AtomicLong timeoutCount = new AtomicLong(0);
  private final AtomicLong errorCount = new AtomicLong(0);

  private final AtomicLong priorRequestCount = new AtomicLong(0);
  // both get、put and getBulk will increase request count by 1
  private final AtomicLong totalRequestCount = new AtomicLong(0);

  private RedisCacheConfig.DurationConfig expiration;

  protected AbstractRedisCache(RedisCacheConfig config)
  {
    this.expiration = config.getExpiration();
  }

  @Override
  public byte[] get(NamedKey key)
  {
    totalRequestCount.incrementAndGet();
    try {
      byte[] bytes = getFromRedis(key.toByteArray());
      if (bytes == null) {
        missCount.incrementAndGet();
        return null;
      } else {
        hitCount.incrementAndGet();
        return bytes;
      }
    }
    catch (JedisException e) {
      if (e.getMessage().contains("Read timed out")) {
        timeoutCount.incrementAndGet();
      } else {
        errorCount.incrementAndGet();
      }
      log.warn(e, "Exception pulling item from cache");
      return null;
    }
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    totalRequestCount.incrementAndGet();
    try {
      this.putToRedis(key.toByteArray(), value, this.expiration);
    }
    catch (JedisException e) {
      errorCount.incrementAndGet();
      log.warn(e, "Exception pushing item to cache");
    }
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    totalRequestCount.incrementAndGet();
    Map<NamedKey, byte[]> results = new HashMap<>();

    try {
      List<NamedKey> namedKeys = Lists.newArrayList(keys);
      List<byte[]> byteKeys = Lists.transform(namedKeys, NamedKey::toByteArray);

      List<byte[]> byteValues = this.mgetFromRedis(byteKeys.toArray(new byte[0][]));
      for (int i = 0; i < byteValues.size(); ++i) {
        if (byteValues.get(i) != null) {
          results.put(namedKeys.get(i), byteValues.get(i));
        }
      }

      hitCount.addAndGet(results.size());
      missCount.addAndGet(namedKeys.size() - results.size());
    }
    catch (JedisException e) {
      if (e.getMessage().contains("Read timed out")) {
        timeoutCount.incrementAndGet();
      } else {
        errorCount.incrementAndGet();
      }
      log.warn(e, "Exception pulling items from cache");
    }

    return results;
  }

  @Override
  public void close(String namespace)
  {
    // no resources to cleanup
  }

  @Override
  @LifecycleStop
  public void close()
  {
    cleanup();
  }

  @Override
  public CacheStats getStats()
  {
    return new CacheStats(
        hitCount.get(),
        missCount.get(),
        0,
        0,
        0,
        timeoutCount.get(),
        errorCount.get()
    );
  }

  @Override
  public boolean isLocal()
  {
    return false;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    final long priorCount = priorRequestCount.get();
    final long totalCount = totalRequestCount.get();
    final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    emitter.emit(builder.build("query/cache/redis/total/requests", totalCount));
    emitter.emit(builder.build("query/cache/redis/delta/requests", totalCount - priorCount));
    if (!priorRequestCount.compareAndSet(priorCount, totalCount)) {
      log.error("Prior value changed while I was reporting! updating anyways");
      priorRequestCount.set(totalCount);
    }
  }

  protected abstract byte[] getFromRedis(byte[] key);

  protected abstract void putToRedis(byte[] key, byte[] value, RedisCacheConfig.DurationConfig expiration);

  protected abstract List<byte[]> mgetFromRedis(byte[]... keys);

  protected abstract void cleanup();
}
