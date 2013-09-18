/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.collections;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.guava.DefaultingHashMap;

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
