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

import com.metamx.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
*/
class ByteCountingLRUMap extends LinkedHashMap<ByteBuffer, byte[]>
{
  private static final Logger log = new Logger(ByteCountingLRUMap.class);

  private final boolean logEvictions;
  private final int logEvictionCount;
  private final long sizeInBytes;

  private volatile long numBytes;
  private volatile long evictionCount;

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
    numBytes = 0;
    evictionCount = 0;
  }

  public long getNumBytes()
  {
    return numBytes;
  }

  public long getEvictionCount()
  {
    return evictionCount;
  }

  @Override
  public byte[] put(ByteBuffer key, byte[] value)
  {
    numBytes += key.remaining() + value.length;
    return super.put(key, value);
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<ByteBuffer, byte[]> eldest)
  {
    if (numBytes > sizeInBytes) {
      ++evictionCount;
      if (logEvictions && evictionCount % logEvictionCount == 0) {
        log.info(
            "Evicting %,dth element.  Size[%,d], numBytes[%,d], averageSize[%,d]",
            evictionCount,
            size(),
            numBytes,
            numBytes / size()
        );
      }

      numBytes -= eldest.getKey().remaining() + eldest.getValue().length;
      return true;
    }
    return false;
  }

  @Override
  public byte[] remove(Object key)
  {
    byte[] value = super.remove(key);
    if(value != null) {
      numBytes -= ((ByteBuffer)key).remaining() + value.length;
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
    numBytes = 0;
    super.clear();
  }
}
