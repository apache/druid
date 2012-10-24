package com.metamx.druid.collect;

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
