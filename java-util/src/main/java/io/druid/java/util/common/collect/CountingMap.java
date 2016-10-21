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

package io.druid.java.util.common.collect;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

// Can't find a good way to abstract over which counter representation is used,
// so I just pick Long/MutableLong.
public class CountingMap<K> extends AbstractMap<K, Long>
{
  private final HashMap<K, AtomicLong> counts = new HashMap<>();

  public void add(K k, Long n)
  {
    if (!counts.containsKey(k)) {
      counts.put(k, new AtomicLong(0));
    }
    counts.get(k).addAndGet(n);
  }

  public Set<Entry<K, Long>> entrySet()
  {
    return Maps.transformValues(
        counts,
        new Function<AtomicLong, Long>()
        {
          @Override
          public Long apply(AtomicLong n)
          {
            return n.get();
          }
        }
    ).entrySet();
  }
}
