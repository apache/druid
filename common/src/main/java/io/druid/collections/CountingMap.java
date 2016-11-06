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

package io.druid.collections;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;

import io.druid.java.util.common.guava.DefaultingHashMap;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class CountingMap<K> extends DefaultingHashMap<K, AtomicLong>
{
  public CountingMap()
  {
    super(new Supplier<AtomicLong>()
    {
      @Override
      public AtomicLong get()
      {
        return new AtomicLong(0);
      }
    });
  }

  public long add(K key, long value)
  {
    return get(key).addAndGet(value);
  }

  public Map<K, Long> snapshot()
  {
    final ImmutableMap.Builder<K, Long> builder = ImmutableMap.builder();

    for (Map.Entry<K, AtomicLong> entry : entrySet()) {
      builder.put(entry.getKey(), entry.getValue().get());
    }

    return builder.build();
  }
}
