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

package io.druid.client.cache;

import com.metamx.common.logger.Logger;

import java.nio.ByteBuffer;
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
   * We want keySet().iterator().remove() to account for object removal
   * The underlying Map calls this.remove(key) so we do not need to override this
   */
  @Override
  public Set<ByteBuffer> keySet()
  {
    return super.keySet();
  }

  @Override
  public void clear()
  {
    numBytes = 0;
    super.clear();
  }
}
