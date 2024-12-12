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

package org.apache.druid.server.lookup.cache.polling;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.druid.query.extraction.MapLookupExtractor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OnHeapPollingCache<K, V> implements PollingCache<K, V>
{
  private final ImmutableMap<K, V> immutableMap;
  private final ImmutableMap<V, List<K>> immutableReverseMap;


  public OnHeapPollingCache(Iterable<Map.Entry<K, V>> entries)
  {

    if (entries == null) {
      immutableMap = ImmutableMap.of();
      immutableReverseMap = ImmutableMap.of();
    } else {
      ImmutableSet.Builder<V> setOfValuesBuilder = ImmutableSet.builder();
      ImmutableMap.Builder<K, V> mapBuilder = ImmutableMap.builder();
      for (Map.Entry<K, V> entry : entries) {
        setOfValuesBuilder.add(entry.getValue());
        mapBuilder.put(entry.getKey(), entry.getValue());
      }
      final Set<V> setOfValues = setOfValuesBuilder.build();
      immutableMap = mapBuilder.build();
      immutableReverseMap = ImmutableMap.copyOf(
          Maps.asMap(
              setOfValues,
              val -> immutableMap
                  .keySet()
                  .stream()
                  .filter(key -> {
                    V retVal = immutableMap.get(key);
                    return retVal != null && retVal.equals(val);
                  })
                  .collect(Collectors.toList())
          )
      );
    }

  }

  @Override
  public V get(K key)
  {
    return immutableMap.get(key);
  }

  @Override
  public List<K> getKeys(final V value)
  {
    final List<K> listOfKeys = immutableReverseMap.get(value);
    if (listOfKeys == null) {
      return Collections.emptyList();
    }
    return listOfKeys;
  }

  @Override
  @SuppressWarnings("unchecked")
  public long estimateHeapFootprint()
  {
    return MapLookupExtractor.estimateHeapFootprint(((Map<String, String>) immutableMap).entrySet());
  }

  @Override
  public void close()
  {
    //noop
  }


  public static class OnHeapPollingCacheProvider<K, V> implements PollingCacheFactory<K, V>
  {
    @Override
    public PollingCache makeOf(Iterable<Map.Entry<K, V>> entries)
    {
      return new OnHeapPollingCache(entries);
    }
  }
}
