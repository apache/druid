package com.metamx.druid.client.cache;

import com.metamx.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

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
    byte[] retVal = super.put(key, value);
    return retVal;
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
}
