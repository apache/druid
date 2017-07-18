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

import io.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
*/
class ByteCountingLRUMap extends LinkedHashMap<ByteBuffer, byte[]>
{
  private static final Logger log = new Logger(ByteCountingLRUMap.class);

  private final boolean logEvictions;
  private final int logEvictionCount;
  private final long sizeInBytes;

  private final AtomicLong numBytes;
  private final AtomicLong evictionCount;

  public ByteCountingLRUMap(
      final long sizeInBytes
  )
  {
    this(16, 0, sizeInBytes);
  }

  public ByteCountingLRUMap(
      final int initialSize,
      final int logEvictionCount,
      final long sizeInBytes
  )
  {
    super(initialSize, 0.75f, true);
    this.logEvictionCount = logEvictionCount;
    this.sizeInBytes = sizeInBytes;

    logEvictions = logEvictionCount != 0;
    numBytes = new AtomicLong(0L);
    evictionCount = new AtomicLong(0L);
  }

  public long getNumBytes()
  {
    return numBytes.get();
  }

  public long getEvictionCount()
  {
    return evictionCount.get();
  }

  @Override
  public byte[] put(ByteBuffer key, byte[] value)
  {
    numBytes.addAndGet(key.remaining() + value.length);
    Iterator<Map.Entry<ByteBuffer, byte[]>> it = entrySet().iterator();
    List<ByteBuffer> keysToRemove = new ArrayList<>();
    long totalEvictionSize = 0L;
    while (numBytes.get() - totalEvictionSize > sizeInBytes && it.hasNext()) {
      evictionCount.incrementAndGet();
      if (logEvictions && evictionCount.get() % logEvictionCount == 0) {
        log.info(
            "Evicting %,dth element.  Size[%,d], numBytes[%,d], averageSize[%,d]",
            evictionCount.get(),
            size(),
            numBytes.get(),
            numBytes.get() / size()
        );
      }

      Map.Entry<ByteBuffer, byte[]> next = it.next();
      totalEvictionSize += next.getKey().remaining() + next.getValue().length;
      keysToRemove.add(next.getKey());
    }

    for (ByteBuffer keyToRemove : keysToRemove) {
      remove(keyToRemove);
    }

    byte[] old = super.put(key, value);
    if (old != null) {
      numBytes.addAndGet(-key.remaining() - old.length);
    }
    return old;
  }

  @Override
  public byte[] remove(Object key)
  {
    byte[] value = super.remove(key);
    if(value != null) {
      long delta = -((ByteBuffer)key).remaining() - value.length;
      numBytes.addAndGet(delta);
    }
    return value;
  }

  /**
   * Don't allow key removal using the underlying keySet iterator
   * All removal operations must use ByteCountingLRUMap.remove()
   */
  @Override
  public Set<ByteBuffer> keySet()
  {
    return Collections.unmodifiableSet(super.keySet());
  }

  @Override
  public void clear()
  {
    numBytes.set(0L);
    super.clear();
  }
}
