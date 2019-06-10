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

import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class HybridCache implements Cache
{
  private static final Logger log = new Logger(HybridCache.class);

  private final HybridCacheConfig config;
  private final Cache level1;
  private final Cache level2;

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);

  public HybridCache(HybridCacheConfig config, Cache level1, Cache level2)
  {
    this.config = config;
    log.info("Config: %s", config);
    this.level1 = level1;
    this.level2 = level2;
  }

  @Nullable
  @Override
  public byte[] get(NamedKey key)
  {
    byte[] res = level1.get(key);
    if (res == null) {
      res = getL2(key);
      if (res != null) {
        level1.put(key, res);
      }
    }
    if (res != null) {
      hitCount.incrementAndGet();
      return res;
    } else {
      missCount.incrementAndGet();
      return null;
    }
  }

  @Nullable
  private byte[] getL2(NamedKey key)
  {
    if (config.getUseL2()) {
      return level2.get(key);
    } else {
      return null;
    }
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    level1.put(key, value);
    if (config.getPopulateL2()) {
      level2.put(key, value);
    }
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    Set<NamedKey> remaining = Sets.newHashSet(keys);
    Map<NamedKey, byte[]> res = level1.getBulk(keys);
    hitCount.addAndGet(res.size());

    remaining = Sets.difference(remaining, res.keySet());

    if (!remaining.isEmpty()) {
      Map<NamedKey, byte[]> res2 = getBulkL2(remaining);
      for (Map.Entry<NamedKey, byte[]> entry : res2.entrySet()) {
        level1.put(entry.getKey(), entry.getValue());
      }

      int size = res2.size();
      hitCount.addAndGet(size);
      missCount.addAndGet(remaining.size() - size);

      if (size != 0) {
        res = new HashMap<>(res);
        res.putAll(res2);
      }
    }
    return res;
  }

  private Map<NamedKey, byte[]> getBulkL2(Iterable<NamedKey> keys)
  {
    if (config.getUseL2()) {
      return level2.getBulk(keys);
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public void close(String namespace)
  {
    Throwable t = null;
    try {
      level1.close(namespace);
    }
    catch (Throwable t1) {
      t = t1;
      throw t1;
    }
    finally {
      if (t != null) {
        try {
          level2.close(namespace);
        }
        catch (Throwable t2) {
          t.addSuppressed(t2);
        }
      } else {
        level2.close(namespace);
      }
    }
  }

  @Override
  @LifecycleStop
  public void close() throws IOException
  {
    CloseableUtils.closeBoth(level1, level2);
  }

  @Override
  public CacheStats getStats()
  {
    CacheStats stats1 = level1.getStats();
    CacheStats stats2 = level2.getStats();
    return new CacheStats(
        hitCount.get(),
        missCount.get(),
        stats1.getNumEntries() + stats2.getNumEntries(),
        stats1.getSizeInBytes() + stats2.getSizeInBytes(),
        stats1.getNumEvictions() + stats2.getNumEvictions(),
        stats1.getNumTimeouts() + stats2.getNumTimeouts(),
        stats1.getNumErrors() + stats2.getNumErrors()
    );
  }

  @Override
  public boolean isLocal()
  {
    return level1.isLocal() && level2.isLocal();
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    level1.doMonitor(emitter);
    level2.doMonitor(emitter);
  }
}
