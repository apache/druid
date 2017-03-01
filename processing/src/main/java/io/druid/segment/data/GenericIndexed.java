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

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import io.druid.common.utils.SerializerUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.smoosh.PositionalMemoryRegion;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import it.unimi.dsi.fastutil.bytes.ByteArrays;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A generic, flat storage mechanism.  Use static methods fromArray() or fromIterable() to construct.  If input
 * is sorted, supports binary search index lookups.  If input is not sorted, only supports array-like index lookups.
 * <p>
 * V1 Storage Format:
 * <p>
 * byte 1: version (0x1)
 * byte 2 == 0x1 =>; allowReverseLookup
 * bytes 3-6 =>; numBytesUsed
 * bytes 7-10 =>; numElements
 * bytes 10-((numElements * 4) + 10): integers representing *end* offsets of byte serialized values
 * bytes ((numElements * 4) + 10)-(numBytesUsed + 2): 4-byte integer representing length of value, followed by bytes
 * for value
 * <p>
 * V2 Storage Format
 * Meta, header and value files are separate and header file stored in native endian byte order.
 * Meta File:
 * byte 1: version (0x2)
 * byte 2 == 0x1 =>; allowReverseLookup
 * bytes 3-6: numberOfElementsPerValueFile expressed as power of 2. That means all the value files contains same
 * number of items except last value file and may have fewer elements.
 * bytes 7-10 =>; numElements
 * bytes 11-14 =>; columnNameLength
 * bytes 15-columnNameLength =>; columnName
 * <p>
 * Header file name is identified as:  String.format("%s_header", columnName)
 * value files are identified as: String.format("%s_value_%d", columnName, fileNumber)
 * number of value files == numElements/numberOfElementsPerValueFile
 */
public class GenericIndexed<T> implements Indexed<T>
{
  public static final byte VERSION_ONE = 0x1;
  public static final byte VERSION_TWO = 0x2;
  private static final byte REVERSE_LOOKUP_ALLOWED = 0x1;
  private final static Ordering<String> NATURAL_STRING_ORDERING = Ordering.natural().nullsFirst();
  private static final SerializerUtils SERIALIZER_UTILS = new SerializerUtils();

  public static final ObjectStrategy<String> STRING_STRATEGY = new CacheableObjectStrategy<String>()
  {
    @Override
    public Class<? extends String> getClazz()
    {
      return String.class;
    }

    @Override public String fromMemory(Memory memory)
    {
      return StringUtils.fromUtf8(memory);
    }

    @Override
    public byte[] toBytes(String val)
    {
      if (val == null) {
        return ByteArrays.EMPTY_ARRAY;
      }
      return StringUtils.toUtf8(val);
    }

    @Override
    public int compare(String o1, String o2)
    {
      return NATURAL_STRING_ORDERING.compare(o1, o2);
    }
  };

  private final ObjectStrategy<T> strategy;
  private final boolean allowReverseLookup;
  private final int size;
  private final BufferIndexed bufferIndexed;
  private final List<Memory> valueBuffers;
  private final Memory headerBuffer;
  private int logBaseTwoOfElementsPerValueFile;

  private Memory memory;

  // used for single file version, v1
  GenericIndexed(
      Memory memory,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup
  )
  {
    this.memory = memory;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;
    size = Integer.reverseBytes(memory.getInt(0));

    int indexOffset = Ints.BYTES;
    int valuesOffset = Ints.BYTES + size * Ints.BYTES;

    valueBuffers = Lists.newArrayList();
    valueBuffers.add(new MemoryRegion(memory, valuesOffset, memory.getCapacity() - valuesOffset));
    headerBuffer = new MemoryRegion(memory, indexOffset, valuesOffset - indexOffset);
    bufferIndexed = new BufferIndexed()
    {
      @Override
      public T get(int index)
      {
        checkIndex(index, size);

        final int startOffset;
        final int endOffset;

        if (index == 0) {
          startOffset = 4;
          endOffset = Integer.reverseBytes(headerBuffer.getInt(0));
        } else {
          int headerPosition = (index - 1) * Ints.BYTES;
          startOffset = Integer.reverseBytes(headerBuffer.getInt(headerPosition)) + Ints.BYTES;
          endOffset = Integer.reverseBytes(headerBuffer.getInt(headerPosition + Ints.BYTES));
        }
        return _get(new MemoryRegion(valueBuffers.get(0), startOffset, endOffset - startOffset));
      }
    };
  }

  // used for multiple file version, v2.
  GenericIndexed(
      List<Memory> valueBuffs,
      Memory headerBuff,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup,
      int logBaseTwoOfElementsPerValueFile,
      int numWritten
  )
  {
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;
    this.valueBuffers = valueBuffs;
    this.headerBuffer = headerBuff;
    this.size = numWritten;
    this.logBaseTwoOfElementsPerValueFile = logBaseTwoOfElementsPerValueFile;
    bufferIndexed = new BufferIndexed()
    {
      @Override
      public T get(int index)
      {
        int fileNum = index >> GenericIndexed.this.logBaseTwoOfElementsPerValueFile;
        final Memory copyBuffer = valueBuffers.get(fileNum);

        checkIndex(index, size);

        final int startOffset;
        final int endOffset;
        int relativePositionOfIndex = index & ((1 << GenericIndexed.this.logBaseTwoOfElementsPerValueFile) - 1);
        if (relativePositionOfIndex == 0) {
          int headerPosition = index * Ints.BYTES;
          startOffset = 4;
          endOffset = headerBuffer.getInt(headerPosition);
        } else {
          int headerPosition = (index - 1) * Ints.BYTES;
          startOffset = headerBuffer.getInt(headerPosition) + 4;
          endOffset = headerBuffer.getInt(headerPosition + 4);
        }
        return _get(new MemoryRegion(copyBuffer, startOffset, endOffset - startOffset));
      }
    };
  }

  public static int getNumberOfFilesRequired(int bagSize, long numWritten)
  {
    int numberOfFilesRequired = (int) (numWritten / bagSize);
    if ((numWritten % bagSize) != 0) {
      numberOfFilesRequired += 1;
    }
    return numberOfFilesRequired;
  }

  /**
   * Checks  if {@code index} a valid `element index` in GenericIndexed.
   * Similar to Preconditions.checkElementIndex() except this method throws {@link IAE} with custom error message.
   * <p>
   * Used here to get existing behavior(same error message and exception) of V1 GenericIndexed.
   *
   * @param index index identifying an element of an GenericIndexed.
   * @param size  number of elements.
   */
  private static void checkIndex(int index, int size)
  {
    if (index < 0) {
      throw new IAE("Index[%s] < 0", index);
    }
    if (index >= size) {
      throw new IAE(String.format("Index[%s] >= size[%s]", index, size));
    }
  }

  public static <T> GenericIndexed<T> fromArray(T[] objects, ObjectStrategy<T> strategy)
  {
    return fromIterable(Arrays.asList(objects), strategy);
  }

  public static <T> GenericIndexed<T> fromIterable(Iterable<T> objectsIterable, ObjectStrategy<T> strategy)
  {
    Iterator<T> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      return new GenericIndexed<T>(new NativeMemory(new byte[]{0,0,0,0}), strategy, true);
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

    return new GenericIndexed<T>(new NativeMemory(theBuffer), strategy, allowReverseLookup);
  }

  public static <T> GenericIndexed<T> read(PositionalMemoryRegion pMemory, ObjectStrategy<T> strategy)
  {
    byte versionFromBuffer = pMemory.getByte();

    if (VERSION_ONE == versionFromBuffer) {
      return createVersionOneGenericIndexed(pMemory, strategy);
    } else if (VERSION_TWO == versionFromBuffer) {
      throw new IAE(
          "use read(ByteBuffer buffer, ObjectStrategy<T> strategy, SmooshedFileMapper fileMapper)"
              + " to read version 2 indexed.",
          versionFromBuffer
      );
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  private static <T> GenericIndexed<T> createVersionOneGenericIndexed(PositionalMemoryRegion pMemory,
      ObjectStrategy<T> strategy)
  {
    boolean allowReverseLookup = pMemory.getByte() == REVERSE_LOOKUP_ALLOWED;
    int size = Integer.reverseBytes(pMemory.getInt());
    PositionalMemoryRegion memoryToUse = pMemory.duplicate();
    memoryToUse.limit(memoryToUse.position() + size);
    pMemory.position(memoryToUse.limit());

    return new GenericIndexed<T>(
        memoryToUse.getRemainingMemory(),
        strategy,
        allowReverseLookup
    );
  }

  private static <T> GenericIndexed<T> createVersionTwoGenericIndexed(
      PositionalMemoryRegion pMemory,
      ObjectStrategy<T> strategy,
      SmooshedFileMapper fileMapper
  )
  {
    if (fileMapper == null) {
      throw new IAE("SmooshedFileMapper can not be null for version 2.");
    }
    boolean allowReverseLookup = pMemory.getByte() == REVERSE_LOOKUP_ALLOWED;
    int logBaseTwoOfElementsPerValueFile = pMemory.getInt();
    int numElements = pMemory.getInt();
    String columnName;

    List<Memory> valueBuffersToUse;
    Memory headerBuffer;
    try {
      columnName = SERIALIZER_UTILS.readString(pMemory);
      valueBuffersToUse = Lists.newArrayList();
      int elementsPerValueFile = 1 << logBaseTwoOfElementsPerValueFile;
      int numberOfFilesRequired = getNumberOfFilesRequired(elementsPerValueFile, numElements);
      for (int i = 0; i < numberOfFilesRequired; i++) {
        valueBuffersToUse.add(
            fileMapper.mapFileToMemory(GenericIndexedWriter.generateValueFileName(columnName, i))
        );
      }
      headerBuffer = fileMapper.mapFileToMemory(GenericIndexedWriter.generateHeaderFileName(columnName));
    }
    catch (IOException e) {
      throw new RuntimeException("File mapping failed.", e);
    }

    return new GenericIndexed<T>(
        valueBuffersToUse,
        headerBuffer,
        strategy,
        allowReverseLookup,
        logBaseTwoOfElementsPerValueFile,
        numElements
    );
  }

  public static <T> GenericIndexed<T> read(PositionalMemoryRegion pMemory,
      ObjectStrategy<T> strategy, SmooshedFileMapper fileMapper)
  {
    byte versionFromBuffer = pMemory.getByte();

    if (VERSION_ONE == versionFromBuffer) {
      return createVersionOneGenericIndexed(pMemory, strategy);
    } else if (VERSION_TWO == versionFromBuffer) {
      return createVersionTwoGenericIndexed(pMemory, strategy, fileMapper);
    }

    throw new IAE("Unknown version [%s]", versionFromBuffer);
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

  public long getSerializedSize()
  {
    if (valueBuffers.size() != 1) {
      throw new UnsupportedOperationException("Method not supported for version 2 GenericIndexed.");
    }
    return memory.getCapacity()
        + 2
        + Ints.BYTES;  //4 bytes to store numbers of bytes
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    //version 2 will always have more than one buffer in valueBuffers.
    if (valueBuffers.size() == 1) {
      channel.write(ByteBuffer.wrap(new byte[]{VERSION_ONE, allowReverseLookup ? (byte) 0x1 : (byte) 0x0}));
      channel.write(ByteBuffer.wrap(Ints.toByteArray((int)memory.getCapacity()))); // 4 Bytes to store size.

      long size = memory.getCapacity();
      while(size > 0){
        int bytes = (int) size;
        if(size > Integer.MAX_VALUE){
          bytes = Integer.MAX_VALUE;
        }
        byte[] byteArray = new byte[bytes];
        memory.getByteArray(0, byteArray, 0, byteArray.length);
        ByteBuffer bb = ByteBuffer.allocate(bytes).put(byteArray);
        bb.flip();
        channel.write(bb);
        size -= bytes;
      }
    } else {
      throw new UnsupportedOperationException(
          "GenericIndexed serialization for V2 is unsupported. Use GenericIndexedWriter instead.");
    }
  }

  /**
   * Create a non-thread-safe Indexed, which may perform better than the underlying Indexed.
   *
   * @return a non-thread-safe Indexed
   */
  public GenericIndexed<T>.BufferIndexed singleThreaded()
  {
    if (valueBuffers.size() == 1) {
      final Memory copyBuffer = valueBuffers.get(0);
      return new BufferIndexed()
      {
        @Override
        public T get(final int index)
        {
          checkIndex(index, size);

          final int startOffset;
          final int endOffset;

          if (index == 0) {
            startOffset = 4;
            endOffset = Integer.reverseBytes(headerBuffer.getInt(0));
          } else {
            int headerPosition = (index - 1) * Ints.BYTES;
            startOffset = Integer.reverseBytes(headerBuffer.getInt(headerPosition)) + 4;
            endOffset = Integer.reverseBytes(headerBuffer.getInt(headerPosition + 4));
          }
          return _get(new MemoryRegion(copyBuffer, startOffset, endOffset - startOffset));
        }
      };
    } else {
      return new BufferIndexed()
      {
        @Override
        public T get(final int index)
        {
          int fileNum = index >> logBaseTwoOfElementsPerValueFile;
          final Memory copyBuffer = valueBuffers.get(fileNum);

          checkIndex(index, size);
          final int startOffset;
          final int endOffset;

          int relativePositionOfIndex = index & ((1 << logBaseTwoOfElementsPerValueFile) - 1);
          if (relativePositionOfIndex == 0) {
            int headerPosition = index * Ints.BYTES;
            startOffset = 4;
            endOffset = headerBuffer.getInt(headerPosition);
          } else {
            int headerPosition = (index - 1) * Ints.BYTES;
            startOffset = headerBuffer.getInt(headerPosition) + 4;
            endOffset = headerBuffer.getInt(headerPosition + 4);
          }

          return _get(new MemoryRegion(memory, startOffset, endOffset - startOffset));
        }
      };
    }

  }

  abstract class BufferIndexed implements Indexed<T>
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

    protected T _get(Memory memory)
    {
      final int size = (int)memory.getCapacity();
      if (size == 0) {
        return null;
      }
      lastReadSize = size;
      // fromByteBuffer must not modify the buffer limit
      return strategy.fromMemory(memory);
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

}
