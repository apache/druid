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

package io.druid.client.cache;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.collections.SerializablePair;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HybridCache implements Cache
{
  private static final Logger log = new Logger(HybridCache.class);

  private final HybridCacheConfig config;
  private final Cache level1;
  private final Cache level2;

  private final LongAdder hitCount = new LongAdder();
  private final LongAdder missCount = new LongAdder();

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
      hitCount.increment();
      return res;
    } else {
      missCount.increment();
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
    hitCount.add(res.size());

    remaining = Sets.difference(remaining, res.keySet());

    if (!remaining.isEmpty()) {
      Map<NamedKey, byte[]> res2 = getBulkL2(remaining);
      for (Map.Entry<NamedKey, byte[]> entry : res2.entrySet()) {
        level1.put(entry.getKey(), entry.getValue());
      }

      int size = res2.size();
      hitCount.add(size);
      missCount.add(remaining.size() - size);

      if (size != 0) {
        res = Maps.newHashMap(res);
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
  public Stream<SerializablePair<NamedKey, Optional<byte[]>>> getBulk(Stream<NamedKey> keys)
  {
    if (!config.getUseL2()) {
      return level1.getBulk(keys);
    }
    final List<SerializablePair<NamedKey, Optional<byte[]>>> materializedL1Results = level1
        .getBulk(keys)
        .collect(Collectors.toList());
    final List<SerializablePair<NamedKey, Optional<byte[]>>> materializedL2Results = level2
        .getBulk(
            materializedL1Results.stream(
            ).filter(
                s -> !s.getRhs().isPresent()
            ).map(
                SerializablePair::getLhs
            )
        ).collect(Collectors.toList());
    // The l2 list should only have "missing" ones from l1. So we loop through and look for the missing L1 results
    // and replace with whatever l2 found
    int l2Pos = 0;
    for (int i = 0; i < materializedL1Results.size(); i++) {
      final SerializablePair<NamedKey, Optional<byte[]>> me = materializedL1Results.get(i);
      if (!me.getRhs().isPresent()) {
        final SerializablePair<NamedKey, Optional<byte[]>> other = materializedL2Results.get(l2Pos++);
        if (!me.getLhs().equals(other.getLhs())) {
          // sanity check for something very broken
          break;
        }
        materializedL1Results.set(i, other);
      }
    }
    // Register hits/misses early so it doesn't require the stream to be consumed
    materializedL1Results.forEach(sp -> {
      if (sp.getRhs().isPresent()) {
        hitCount.increment();
      } else {
        missCount.increment();
      }
    });
    return materializedL1Results.stream();
  }

  @Override
  public void close(String namespace)
  {
    level1.close(namespace);
    level2.close(namespace);
  }

  @Override
  public CacheStats getStats()
  {
    CacheStats stats1 = level1.getStats();
    CacheStats stats2 = level2.getStats();
    return new CacheStats(
        hitCount.longValue(),
        missCount.longValue(),
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
