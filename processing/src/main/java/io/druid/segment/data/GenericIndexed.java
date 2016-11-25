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

import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.store.ByteBufferIndexInput;
import io.druid.segment.store.IndexInput;
import io.druid.segment.store.IndexInputUtil;

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
public class GenericIndexed<T> implements Indexed<T>
{
  private static final byte version = 0x1;

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
        offset += 4 + bytes.length;
        headerBytes.write(Ints.toByteArray(offset));
        valueBytes.write(Ints.toByteArray(bytes.length));
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

    return new GenericIndexed<T>(theBuffer.asReadOnlyBuffer(), strategy, allowReverseLookup);
  }

  /**
   * IndexInput version
   *
   * @param objects
   * @param strategy
   * @param <T>
   *
   * @return
   */
  public static <T> GenericIndexed<T> fromArrayV1(T[] objects, ObjectStrategy<T> strategy)
  {
    return fromIterableV1(Arrays.asList(objects), strategy);
  }

  /**
   * IndexInput version
   *
   * @param objectsIterable
   * @param strategy
   * @param <T>
   *
   * @return
   */
  public static <T> GenericIndexed<T> fromIterableV1(Iterable<T> objectsIterable, ObjectStrategy<T> strategy)
  {
    Iterator<T> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0);
      buffer.flip();
      IndexInput indexInput = new ByteBufferIndexInput(buffer);
      return new GenericIndexed<T>(indexInput, strategy, true);
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
        offset += 4 + bytes.length;
        headerBytes.write(Ints.toByteArray(offset));
        valueBytes.write(Ints.toByteArray(bytes.length));
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


    ByteBuffer generatedIndexByteBuffer = theBuffer.asReadOnlyBuffer();
    IndexInput indexInput = new ByteBufferIndexInput(generatedIndexByteBuffer);

    return new GenericIndexed<T>(indexInput, strategy, allowReverseLookup);
  }

  @Override
  public Class<? extends T> getClazz()
  {
    return indexed.getClazz();
  }

  @Override
  public int size()
  {
    return indexed.size();
  }

  @Override
  public T get(int index)
  {
    return indexed.get(index);
  }

  /**
   * Returns the index of "value" in this GenericIndexed object, or (-(insertion point) - 1) if the value is not
   * present, in the manner of Arrays.binarySearch. This strengthens the contract of Indexed, which only guarantees
   * that values-not-found will return some negative number.
   *
   * @param value value to search for
   * @return index of value, or negative number equal to (-(insertion point) - 1).
   */
  @Override
  public int indexOf(T value)
  {
    return indexed.indexOf(value);
  }

  @Override
  public Iterator<T> iterator()
  {
    return indexed.iterator();
  }

  private final ByteBuffer theBuffer;
  private final ObjectStrategy<T> strategy;
  private final boolean allowReverseLookup;
  private final int size;

  private final int valuesOffset;
  private final BufferIndexed bufferIndexed;


  private final IndexInput indexInput;

  private final Indexed<T> indexed;

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
    bufferIndexed = new BufferIndexed();
    //for new api
    indexInput = null;
    indexed = new BufferIndexed();
  }

  GenericIndexed(
      IndexInput indexInput,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup
  )
  {

    this.theBuffer = null;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;
    this.indexInput = indexInput;
    try {
      size = indexInput.readInt();

      indexOffset = (int) indexInput.getFilePointer();
      valuesOffset = (int) indexInput.getFilePointer() + (size << 2);
      bufferIndexed = null;
      indexed = new IndexInputIndexed();
    }
    catch (IOException e) {
      throw new IAE("error occured to construct a IndexInput version GenericIndexed", e);
    }

  }

  class BufferIndexed implements Indexed<T>
  {
    int lastReadSize;

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
      lastReadSize = size;
      // fromByteBuffer must not modify the buffer limit
      final T value = strategy.fromByteBuffer(copyBuffer, size);

      return value;
    }

    /**
     * This method makes no guarantees with respect to thread safety
     * @return the size in bytes of the last value read
     */
    public int getLastValueSize() {
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

  /**
   * indexed implementation of the new IndexInput API
   */
  class IndexInputIndexed implements Indexed<T>
  {
    int lastReadSize;

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
      return _get(indexInput.duplicate(), index);
    }

    protected T _get(final IndexInput indexInput, final int index)
    {
      if (index < 0) {
        throw new IAE("Index[%s] < 0", index);
      }
      if (index >= size) {
        throw new IAE(String.format("Index[%s] >= size[%s]", index, size));
      }

      final int startOffset;
      final int endOffset;
      try {
        if (index == 0) {
          startOffset = 4;
          indexInput.seek(indexOffset);
          endOffset = indexInput.readInt();
        } else {
          indexInput.seek(indexOffset + ((index - 1) * 4));
          startOffset = indexInput.readInt() + 4;
          endOffset = indexInput.readInt();
        }

        if (startOffset == endOffset) {
          return null;
        }

        indexInput.seek(valuesOffset + startOffset);
        final int size = endOffset - startOffset;
        lastReadSize = size;
        //TODO later refactor the ObjectStrategy with IndexInput
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        byte[] strategyBytes = byteBuffer.array();
        indexInput.readBytes(strategyBytes, 0, size);
        final T value = strategy.fromByteBuffer(byteBuffer, size);
        return value;
      }
      catch (IOException e) {
        throw new IAE("while reading generic index of " + index + " occurs error", e);
      }
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

  /**
   * IndexInput version
   *
   * @return
   */
  public long getSerializedSizeV1()
  {

    long remaining = remaining() + 2 + 4 + 4;
    return remaining;
  }

  private long remaining()
  {
    return IndexInputUtil.remaining(indexInput);
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version, allowReverseLookup ? (byte) 0x1 : (byte) 0x0}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(theBuffer.remaining() + 4)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
    channel.write(theBuffer.asReadOnlyBuffer());
  }

  /**
   * new version
   *
   * @param channel
   *
   * @throws IOException
   */
  public void writeToChannelV1(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version, allowReverseLookup ? (byte) 0x1 : (byte) 0x0}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray((int) remaining() + 4)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
    IndexInput duplicatedInput = indexInput.duplicate();
    IndexInputUtil.write2Channel(duplicatedInput, channel);
  }


  /**
   * Create a non-thread-safe Indexed, which may perform better than the underlying Indexed.
   *
   * @return a non-thread-safe Indexed
   */
  public GenericIndexed<T>.BufferIndexed singleThreaded()
  {
    final ByteBuffer copyBuffer = theBuffer.asReadOnlyBuffer();
    return new BufferIndexed() {
      @Override
      public T get(int index)
      {
        return _get(copyBuffer, index);
      }
    };
  }

  /**
   * new version
   *
   * @return
   */
  public GenericIndexed<T>.IndexInputIndexed singleThreadedV1()
  {
    final IndexInput copyed = indexInput.duplicate();
    return new IndexInputIndexed()
    {
      @Override
      public T get(int index)
      {
        return _get(copyed, index);
      }
    };
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

  /**
   * new API version
   *
   * @param indexInput
   * @param strategy
   * @param <T>
   *
   * @return
   *
   * @throws IOException
   */
  public static <T> GenericIndexed<T> read(IndexInput indexInput, ObjectStrategy<T> strategy) throws IOException
  {
    byte versionFromBuffer = indexInput.readByte();

    if (version == versionFromBuffer) {
      boolean allowReverseLookup = indexInput.readByte() == 0x1;
      int size = indexInput.readInt();
      int currentPosition = (int) indexInput.getFilePointer();
      int refreshPosition = currentPosition + size;
      IndexInput indexInputToUse = indexInput.slice(currentPosition, size);
      indexInput.seek(refreshPosition);
      return new GenericIndexed<T>(
          indexInputToUse,
          strategy,
          allowReverseLookup
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
      return io.druid.java.util.common.StringUtils.fromUtf8(buffer, numBytes);
    }

    @Override
    public byte[] toBytes(String val)
    {
      if (val == null) {
        return new byte[]{};
      }
      return io.druid.java.util.common.StringUtils.toUtf8(val);
    }

    @Override
    public int compare(String o1, String o2)
    {
      return Ordering.natural().nullsFirst().compare(o1, o2);
    }

    /**
     * IndexInput version of ObjectStrategy
     * @param indexInput
     * @param numBytes
     * @return
     */
    public String fromIndexInput(final IndexInput indexInput, final int numBytes)
    {
      try {
        ByteBuffer byteBuffer = ByteBuffer.allocate(numBytes);
        final byte[] bytes = byteBuffer.array();
        indexInput.readBytes(bytes, 0, numBytes);
        return io.druid.java.util.common.StringUtils.fromUtf8(byteBuffer, numBytes);
      }
      catch (IOException e) {
        throw new IAE("");
      }

    }
  };
}
