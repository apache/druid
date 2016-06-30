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

package io.druid.segment.data;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.guava.CloseQuietly;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A generic, flat storage mechanism.  Use static methods fromArray() or fromIterable() to construct.  If input
 * is sorted, supports binary search index lookups.  If input is not sorted, only supports array-like index lookups.
 * <p/>
 * Storage Format:
 * <p/>
 * byte 1: version (0x1 or 0x2)
 * byte 2 == 0x1 =&gt; allowReverseLookup
 * bytes 3-6 =&gt; numBytesUsed
 * bytes 7-10 =&gt; numElements
 * bytes 10-((numElements * 4) + 10): integers representing *end* offsets of byte serialized values
 * bytes ((numElements * 4) + 10)-(numBytesUsed + 2):
 * For version 0x1: 4-byte integer representing length of value, followed by bytes for value
 * For version 0x2: consecutive bytes for the values
 */
public class GenericIndexed<T> implements Indexed<T>
{
  private static final byte version1 = 0x1;
  private static final byte version2 = 0x2; //current version used for writing

  private int indexOffset;

  public static <T> GenericIndexed<T> fromArray(T[] objects, ObjectStrategy<T> strategy, final byte version)
  {
    return fromIterable(Arrays.asList(objects), strategy, version);
  }

  public static <T> GenericIndexed<T> fromIterable(
      Iterable<T> objectsIterable,
      ObjectStrategy<T> strategy,
      final byte version
  )
  {
    Preconditions.checkArgument(
        version == version1 || version == version2,
        String.format("Unknown GenericIndexed version [%d]", version)
    );
    final int valueLength = version == version2 ? 0 : 4;

    Iterator<T> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0);
      buffer.flip();
      return new GenericIndexed<T>(buffer, strategy, true, version);
    }

    boolean allowReverseLookup = true;
    int count = 0;

    ByteArrayOutputStream headerBytes = new ByteArrayOutputStream();
    ByteArrayOutputStream valueBytes = new ByteArrayOutputStream();
    try {
      int offset = 0;
      T prevVal = null;
      do {
        count++;
        T next = objects.next();
        if (allowReverseLookup && prevVal != null && !(strategy.compare(prevVal, next) < 0)) {
          allowReverseLookup = false;
        }

        final byte[] bytes = strategy.toBytes(next);

        offset += valueLength + bytes.length;
        headerBytes.write(Ints.toByteArray(offset));
        if (version == version1) {
          valueBytes.write(Ints.toByteArray(bytes.length));
        }
        valueBytes.write(bytes);

        if (prevVal instanceof Closeable) {
          CloseQuietly.close((Closeable) prevVal);
        }
        prevVal = next;
      } while (objects.hasNext());

      if (prevVal instanceof Closeable) {
        CloseQuietly.close((Closeable) prevVal);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    ByteBuffer theBuffer = ByteBuffer.allocate(Ints.BYTES + headerBytes.size() + valueBytes.size());
    theBuffer.put(Ints.toByteArray(count));
    theBuffer.put(headerBytes.toByteArray());
    theBuffer.put(valueBytes.toByteArray());
    theBuffer.flip();

    return new GenericIndexed<T>(theBuffer.asReadOnlyBuffer(), strategy, allowReverseLookup, version);
  }

  @Override
  public Class<? extends T> getClazz()
  {
    return bufferIndexed.getClazz();
  }

  @Override
  public int size()
  {
    return bufferIndexed.size();
  }

  @Override
  public T get(int index)
  {
    return bufferIndexed.get(index);
  }

  /**
   * Returns the index of "value" in this GenericIndexed object, or (-(insertion point) - 1) if the value is not
   * present, in the manner of Arrays.binarySearch. This strengthens the contract of Indexed, which only guarantees
   * that values-not-found will return some negative number.
   *
   * @param value value to search for
   *
   * @return index of value, or negative number equal to (-(insertion point) - 1).
   */
  @Override
  public int indexOf(T value)
  {
    return bufferIndexed.indexOf(value);
  }

  @Override
  public Iterator<T> iterator()
  {
    return bufferIndexed.iterator();
  }

  private final ByteBuffer theBuffer;
  private final ObjectStrategy<T> strategy;
  private final boolean allowReverseLookup;
  private final int size;

  private final int valuesOffset;
  private final BufferIndexed bufferIndexed;

  private final byte version;

  GenericIndexed(
      ByteBuffer buffer,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup,
      byte version
  )
  {
    this.theBuffer = buffer;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;
    this.version = version;

    size = theBuffer.getInt();
    indexOffset = theBuffer.position();
    valuesOffset = theBuffer.position() + (size << 2);
    if (version == version2) {
      bufferIndexed = new BufferIndexed(0);
    } else {
      bufferIndexed = new BufferIndexed(4);
    }
  }

  class BufferIndexed implements Indexed<T>
  {
    int lastReadSize;
    final int valueLength;

    BufferIndexed(int valueLength)
    {
      this.valueLength = valueLength;
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
    public T get(final int index)
    {
      return _get(theBuffer.asReadOnlyBuffer(), index);
    }

    protected T _get(final ByteBuffer copyBuffer, final int index)
    {
      if (index < 0) {
        throw new IAE("Index[%s] < 0", index);
      }
      if (index >= size) {
        throw new IAE(String.format("Index[%s] >= size[%s]", index, size));
      }

      final int startOffset;
      final int endOffset;

      if (index == 0) {
        startOffset = valueLength;
        endOffset = copyBuffer.getInt(indexOffset);
      } else {
        copyBuffer.position(indexOffset + ((index - 1) * 4));
        startOffset = copyBuffer.getInt() + valueLength;
        endOffset = copyBuffer.getInt();
      }

      if (startOffset == endOffset) {
        return null;
      }

      copyBuffer.position(valuesOffset + startOffset);
      final int size = endOffset - startOffset;
      lastReadSize = size;
      // fromByteBuffer must not modify the buffer limit
      final T value = strategy.fromByteBuffer(copyBuffer, size);

      return value;
    }

    /**
     * This method makes no guarantees with respect to thread safety
     *
     * @return the size in bytes of the last value read
     */
    public int getLastValueSize()
    {
      return lastReadSize;
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

    @Override
    public Iterator<T> iterator()
    {
      return IndexedIterable.create(this).iterator();
    }
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
   * Create a non-thread-safe Indexed, which may perform better than the underlying Indexed.
   *
   * @return a non-thread-safe Indexed
   */
  public GenericIndexed<T>.BufferIndexed singleThreaded()
  {
    final ByteBuffer copyBuffer = theBuffer.asReadOnlyBuffer();

    if (version == version2) {
      return new BufferIndexed(0)
      {
        @Override
        public T get(int index)
        {
          return _get(copyBuffer, index);
        }
      };
    } else {
      return new BufferIndexed(4)
      {
        @Override
        public T get(int index)
        {
          return _get(copyBuffer, index);
        }
      };
    }
  }

  public static <T> GenericIndexed<T> read(ByteBuffer buffer, ObjectStrategy<T> strategy)
  {
    final byte versionFromBuffer = buffer.get();

    if (versionFromBuffer == version1 || versionFromBuffer == version2) {
      boolean allowReverseLookup = buffer.get() == 0x1;
      int size = buffer.getInt();
      ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
      bufferToUse.limit(bufferToUse.position() + size);
      buffer.position(bufferToUse.limit());

      return new GenericIndexed<T>(
          bufferToUse,
          strategy,
          allowReverseLookup,
          versionFromBuffer
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public static final ObjectStrategy<String> STRING_STRATEGY = new CacheableObjectStrategy<String>()
  {
    @Override
    public Class<? extends String> getClazz()
    {
      return String.class;
    }

    @Override
    public String fromByteBuffer(final ByteBuffer buffer, final int numBytes)
    {
      return com.metamx.common.StringUtils.fromUtf8(buffer, numBytes);
    }

    @Override
    public byte[] toBytes(String val)
    {
      if (val == null) {
        return new byte[]{};
      }
      return com.metamx.common.StringUtils.toUtf8(val);
    }

    @Override
    public int compare(String o1, String o2)
    {
      return Ordering.natural().nullsFirst().compare(o1, o2);
    }
  };
}
