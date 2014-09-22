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

package io.druid.segment.data;

import com.google.common.base.Charsets;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A generic, flat storage mechanism.  Use static methods fromArray() or fromIterable() to construct.  If input
 * is sorted, supports binary search index lookups.  If input is not sorted, only supports array-like index lookups.
 * 
 * V1 Storage Format:
 * 
 * byte 1: version (0x1)
 * byte 2 == 0x1 =&gt; allowReverseLookup
 * bytes 3-6 =&gt; numBytesUsed
 * bytes 7-10 =&gt; numElements
 * bytes 10-((numElements * 4) + 10): integers representing *end* offsets of byte serialized values
 * bytes ((numElements * 4) + 10)-(numBytesUsed + 2): 4-byte integer representing length of value, followed by bytes for value
 */
public class GenericIndexed<T> implements Indexed<T>, Closeable
{
  private static final Logger log = new Logger(GenericIndexed.class);

  private static final byte version = 0x1;

  public static final int INITIAL_CACHE_CAPACITY = 16384;

  private int indexOffset;

  public static <T> GenericIndexed<T> fromArray(T[] objects, ObjectStrategy<T> strategy)
  {
    return fromIterable(Arrays.asList(objects), strategy);
  }

  public static <T> GenericIndexed<T> fromIterable(Iterable<T> objectsIterable, ObjectStrategy<T> strategy)
  {
    Iterator<T> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0);
      buffer.flip();
      return new GenericIndexed<T>(buffer, strategy, true);
    }

    boolean allowReverseLookup = true;
    int count = 1;
    T prevVal = objects.next();
    while (objects.hasNext()) {
      T next = objects.next();
      if (!(strategy.compare(prevVal, next) < 0)) {
        allowReverseLookup = false;
      }
      if (prevVal instanceof Closeable) {
        CloseQuietly.close((Closeable) prevVal);
      }

      prevVal = next;
      ++count;
    }
    if (prevVal instanceof Closeable) {
      CloseQuietly.close((Closeable) prevVal);
    }

    ByteArrayOutputStream headerBytes = new ByteArrayOutputStream(4 + (count * 4));
    ByteArrayOutputStream valueBytes = new ByteArrayOutputStream();
    int offset = 0;

    try {
      headerBytes.write(Ints.toByteArray(count));

      for (T object : objectsIterable) {
        final byte[] bytes = strategy.toBytes(object);
        offset += 4 + bytes.length;
        headerBytes.write(Ints.toByteArray(offset));
        valueBytes.write(Ints.toByteArray(bytes.length));
        valueBytes.write(bytes);

        if (object instanceof Closeable) {
          CloseQuietly.close((Closeable) object);
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    ByteBuffer theBuffer = ByteBuffer.allocate(headerBytes.size() + valueBytes.size());
    theBuffer.put(headerBytes.toByteArray());
    theBuffer.put(valueBytes.toByteArray());
    theBuffer.flip();

    return new GenericIndexed<T>(theBuffer.asReadOnlyBuffer(), strategy, allowReverseLookup);
  }

  private static class SizedLRUMap<K, V> extends LinkedHashMap<K, Pair<Integer, V>>
  {
    private final int maxBytes;
    private int numBytes = 0;

    public SizedLRUMap(int initialCapacity, int maxBytes)
    {
      super(initialCapacity, 0.75f, true);
      this.maxBytes = maxBytes;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, Pair<Integer, V>> eldest)
    {
      if (numBytes > maxBytes) {
        numBytes -= eldest.getValue().lhs;
        return true;
      }
      return false;
    }

    public void put(K key, V value, int size)
    {
      final int totalSize = size + 48; // add approximate object overhead
      numBytes += totalSize;
      super.put(key, new Pair<>(totalSize, value));
    }

    public V getValue(Object key)
    {
      final Pair<Integer, V> sizeValuePair = super.get(key);
      return sizeValuePair == null ? null : sizeValuePair.rhs;
    }
  }

  private final ByteBuffer theBuffer;
  private final ObjectStrategy<T> strategy;
  private final boolean allowReverseLookup;
  private final int size;

  private final boolean cacheable;
  private final ThreadLocal<ByteBuffer> cachedBuffer;
  private final ThreadLocal<SizedLRUMap<Integer, T>> cachedValues;
  private final int valuesOffset;

  GenericIndexed(
      ByteBuffer buffer,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup
  )
  {
    this.theBuffer = buffer;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;

    size = theBuffer.getInt();
    indexOffset = theBuffer.position();
    valuesOffset = theBuffer.position() + (size << 2);

    this.cachedBuffer = new ThreadLocal<ByteBuffer>()
    {
      @Override
      protected ByteBuffer initialValue()
      {
        return theBuffer.asReadOnlyBuffer();
      }
    };

    this.cacheable = false;
    this.cachedValues = new ThreadLocal<>();
  }

  /**
   * Creates a copy of the given indexed with the given cache size
   * The resulting copy must be closed to release resources used by the cache
   */
  GenericIndexed(GenericIndexed<T> other, final int maxBytes)
  {
    this.theBuffer = other.theBuffer;
    this.strategy = other.strategy;
    this.allowReverseLookup = other.allowReverseLookup;
    this.size = other.size;
    this.indexOffset = other.indexOffset;
    this.valuesOffset = other.valuesOffset;
    this.cachedBuffer = other.cachedBuffer;

    this.cachedValues = new ThreadLocal<SizedLRUMap<Integer, T>>() {
      @Override
      protected SizedLRUMap<Integer, T> initialValue()
      {
        log.debug("Allocating column cache of max size[%d]", maxBytes);
        return new SizedLRUMap<>(INITIAL_CACHE_CAPACITY, maxBytes);
      }
    };

    this.cacheable = strategy instanceof CacheableObjectStrategy;
  }

  @Override
  public Class<? extends T> getClazz()
  {
    return strategy.getClazz();
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public T get(int index)
  {
    if (index < 0) {
      throw new IAE("Index[%s] < 0", index);
    }
    if (index >= size) {
      throw new IAE(String.format("Index[%s] >= size[%s]", index, size));
    }

    if(cacheable) {
      final T cached = cachedValues.get().getValue(index);
      if (cached != null) {
        return cached;
      }
    }

    // using a cached copy of the buffer instead of making a read-only copy every time get() is called is faster
    final ByteBuffer copyBuffer = this.cachedBuffer.get();

    final int startOffset;
    final int endOffset;

    if (index == 0) {
      startOffset = 4;
      endOffset = copyBuffer.getInt(indexOffset);
    } else {
      copyBuffer.position(indexOffset + ((index - 1) * 4));
      startOffset = copyBuffer.getInt() + 4;
      endOffset = copyBuffer.getInt();
    }

    if (startOffset == endOffset) {
      return null;
    }

    copyBuffer.position(valuesOffset + startOffset);
    final int size = endOffset - startOffset;
    // fromByteBuffer must not modify the buffer limit
    final T value = strategy.fromByteBuffer(copyBuffer, size);

    if(cacheable) {
      cachedValues.get().put(index, value, size);
    }
    return value;
  }

  @Override
  public int indexOf(T value)
  {
    if (!allowReverseLookup) {
      throw new UnsupportedOperationException("Reverse lookup not allowed.");
    }

    value = (value != null && value.equals("")) ? null : value;

    int minIndex = 0;
    int maxIndex = size - 1;
    while (minIndex <= maxIndex) {
      int currIndex = (minIndex + maxIndex) >>> 1;

      T currValue = get(currIndex);
      int comparison = strategy.compare(currValue, value);
      if (comparison == 0) {
        return currIndex;
      }

      if (comparison < 0) {
        minIndex = currIndex + 1;
      } else {
        maxIndex = currIndex - 1;
      }
    }

    return -(minIndex + 1);
  }

  public long getSerializedSize()
  {
    return theBuffer.remaining() + 2 + 4 + 4;
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version, allowReverseLookup ? (byte) 0x1 : (byte) 0x0}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(theBuffer.remaining() + 4)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
    channel.write(theBuffer.asReadOnlyBuffer());
  }

  /**
   * The returned GenericIndexed must be closed to release the underlying memory
   *
   * @param maxBytes maximum size in bytes of the lookup cache
   * @return a copy of this GenericIndexed with a lookup cache.
   */
  public GenericIndexed<T> withCache(int maxBytes)
  {
    return new GenericIndexed<>(this, maxBytes);
  }

  @Override
  public void close() throws IOException
  {
    if(cacheable) {
      log.debug("Closing column cache");
      cachedValues.get().clear();
      cachedValues.remove();
    }
  }

  public static <T> GenericIndexed<T> read(ByteBuffer buffer, ObjectStrategy<T> strategy)
  {
    byte versionFromBuffer = buffer.get();

    if (version == versionFromBuffer) {
      boolean allowReverseLookup = buffer.get() == 0x1;
      int size = buffer.getInt();
      ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
      bufferToUse.limit(bufferToUse.position() + size);
      buffer.position(bufferToUse.limit());

      return new GenericIndexed<T>(
          bufferToUse,
          strategy,
          allowReverseLookup
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public static ObjectStrategy<String> stringStrategy = new CacheableObjectStrategy<String>()
  {
    @Override
    public Class<? extends String> getClazz()
    {
      return String.class;
    }

    @Override
    public String fromByteBuffer(final ByteBuffer buffer, final int numBytes)
    {
      final byte[] bytes = new byte[numBytes];
      buffer.get(bytes);
      return new String(bytes, Charsets.UTF_8);
    }

    @Override
    public byte[] toBytes(String val)
    {
      if (val == null) {
        return new byte[]{};
      }
      return val.getBytes(Charsets.UTF_8);
    }

    @Override
    public int compare(String o1, String o2)
    {
      return Ordering.natural().nullsFirst().compare(o1, o2);
    }
  };

  @Override
  public Iterator<T> iterator()
  {
    return IndexedIterable.create(this).iterator();
  }
}
