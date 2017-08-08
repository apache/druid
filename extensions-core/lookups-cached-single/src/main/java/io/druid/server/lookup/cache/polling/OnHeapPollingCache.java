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

package io.druid.server.lookup.cache.polling;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
      for (Map.Entry<K, V> entry: entries
           ) {
        setOfValuesBuilder.add(entry.getValue());
        mapBuilder.put(entry.getKey(), entry.getValue());
      }
      final Set<V> setOfValues = setOfValuesBuilder.build();
      immutableMap = mapBuilder.build();
      immutableReverseMap = ImmutableMap.copyOf(Maps.asMap(
          setOfValues, new Function<V, List<K>>()
          {
            @Override
            public List<K> apply(final V input)
            {
              return Lists.newArrayList(Maps.filterKeys(immutableMap, new Predicate<K>()
              {
                @Override
                public boolean apply(K key)
                {
                  V retVal = immutableMap.get(key);
                  if (retVal == null) {
                    return false;
                  }
                  return retVal.equals(input);
                }
              }).keySet());
            }
          }));
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
