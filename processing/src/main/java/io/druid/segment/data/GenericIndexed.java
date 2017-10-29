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

import com.google.common.primitives.Ints;
import io.druid.common.utils.SerializerUtils;
import io.druid.io.ZeroCopyByteArrayOutputStream;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import it.unimi.dsi.fastutil.bytes.ByteArrays;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Iterator;

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
 * Header file name is identified as: StringUtils.format("%s_header", columnName)
 * value files are identified as: StringUtils.format("%s_value_%d", columnName, fileNumber)
 * number of value files == numElements/numberOfElementsPerValueFile
 */
public class GenericIndexed<T> implements Indexed<T>
{
  static final byte VERSION_ONE = 0x1;
  static final byte VERSION_TWO = 0x2;
  static final byte REVERSE_LOOKUP_ALLOWED = 0x1;
  static final byte REVERSE_LOOKUP_DISALLOWED = 0x0;
  private static final SerializerUtils SERIALIZER_UTILS = new SerializerUtils();

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
      return StringUtils.fromUtf8(buffer, numBytes);
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
      return Comparators.<String>naturalNullsFirst().compare(o1, o2);
    }
  };

  public static <T> GenericIndexed<T> read(ByteBuffer buffer, ObjectStrategy<T> strategy)
  {
    byte versionFromBuffer = buffer.get();

    if (VERSION_ONE == versionFromBuffer) {
      return createGenericIndexedVersionOne(buffer, strategy);
    } else if (VERSION_TWO == versionFromBuffer) {
      throw new IAE(
          "use read(ByteBuffer buffer, ObjectStrategy<T> strategy, SmooshedFileMapper fileMapper)"
          + " to read version 2 indexed."
      );
    }
    throw new IAE("Unknown version[%d]", (int) versionFromBuffer);
  }

  public static <T> GenericIndexed<T> read(ByteBuffer buffer, ObjectStrategy<T> strategy, SmooshedFileMapper fileMapper)
  {
    byte versionFromBuffer = buffer.get();

    if (VERSION_ONE == versionFromBuffer) {
      return createGenericIndexedVersionOne(buffer, strategy);
    } else if (VERSION_TWO == versionFromBuffer) {
      return createGenericIndexedVersionTwo(buffer, strategy, fileMapper);
    }

    throw new IAE("Unknown version [%s]", versionFromBuffer);
  }

  public static <T> GenericIndexed<T> fromArray(T[] objects, ObjectStrategy<T> strategy)
  {
    return fromIterable(Arrays.asList(objects), strategy);
  }

  public static <T> GenericIndexed<T> fromIterable(Iterable<T> objectsIterable, ObjectStrategy<T> strategy)
  {
    return fromIterableVersionOne(objectsIterable, strategy);
  }

  static int getNumberOfFilesRequired(int bagSize, long numWritten)
  {
    int numberOfFilesRequired = (int) (numWritten / bagSize);
    if ((numWritten % bagSize) != 0) {
      numberOfFilesRequired += 1;
    }
    return numberOfFilesRequired;
  }


  private final boolean versionOne;

  private final ObjectStrategy<T> strategy;
  private final boolean allowReverseLookup;
  private final int size;
  private final ByteBuffer headerBuffer;

  private final ByteBuffer firstValueBuffer;

  private final ByteBuffer[] valueBuffers;
  private int logBaseTwoOfElementsPerValueFile;
  private int relativeIndexMask;

  private final ByteBuffer theBuffer;

  /**
   * Constructor for version one.
   */
  GenericIndexed(
      ByteBuffer buffer,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup
  )
  {
    this.versionOne = true;

    this.theBuffer = buffer;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;
    size = theBuffer.getInt();

    int indexOffset = theBuffer.position();
    int valuesOffset = theBuffer.position() + size * Ints.BYTES;

    buffer.position(valuesOffset);
    // Ensure the value buffer's limit equals to capacity.
    firstValueBuffer = buffer.slice();
    valueBuffers = new ByteBuffer[]{firstValueBuffer};
    buffer.position(indexOffset);
    headerBuffer = buffer.slice();
  }


  /**
   * Constructor for version two.
   */
  GenericIndexed(
      ByteBuffer[] valueBuffs,
      ByteBuffer headerBuff,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup,
      int logBaseTwoOfElementsPerValueFile,
      int numWritten
  )
  {
    this.versionOne = false;

    this.theBuffer = null;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;
    this.valueBuffers = valueBuffs;
    this.firstValueBuffer = valueBuffers[0];
    this.headerBuffer = headerBuff;
    this.size = numWritten;
    this.logBaseTwoOfElementsPerValueFile = logBaseTwoOfElementsPerValueFile;
    this.relativeIndexMask = (1 << logBaseTwoOfElementsPerValueFile) - 1;
    headerBuffer.order(ByteOrder.nativeOrder());
  }

  /**
   * Checks  if {@code index} a valid `element index` in GenericIndexed.
   * Similar to Preconditions.checkElementIndex() except this method throws {@link IAE} with custom error message.
   * <p>
   * Used here to get existing behavior(same error message and exception) of V1 GenericIndexed.
   *
   * @param index index identifying an element of an GenericIndexed.
   */
  private void checkIndex(int index)
  {
    if (index < 0) {
      throw new IAE("Index[%s] < 0", index);
    }
    if (index >= size) {
      throw new IAE("Index[%d] >= size[%d]", index, size);
    }
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
    return versionOne ? getVersionOne(index) : getVersionTwo(index);
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
    return indexOf(this, value);
  }

  private int indexOf(Indexed<T> indexed, T value)
  {
    if (!allowReverseLookup) {
      throw new UnsupportedOperationException("Reverse lookup not allowed.");
    }

    value = (value != null && value.equals("")) ? null : value;

    int minIndex = 0;
    int maxIndex = size - 1;
    while (minIndex <= maxIndex) {
      int currIndex = (minIndex + maxIndex) >>> 1;

      T currValue = indexed.get(currIndex);
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

  public long getSerializedSize()
  {
    if (!versionOne) {
      throw new UnsupportedOperationException("Method not supported for version 2 GenericIndexed.");
    }
    return getSerializedSizeVersionOne();
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    if (versionOne) {
      writeToChannelVersionOne(channel);
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
    return versionOne ? singleThreadedVersionOne() : singleThreadedVersionTwo();
  }

  private T copyBufferAndGet(ByteBuffer valueBuffer, int startOffset, int endOffset)
  {
    final int size = endOffset - startOffset;
    if (size == 0) {
      return null;
    }
    ByteBuffer copyValueBuffer = valueBuffer.asReadOnlyBuffer();
    copyValueBuffer.position(startOffset);
    // fromByteBuffer must not modify the buffer limit
    return strategy.fromByteBuffer(copyValueBuffer, size);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("versionOne", versionOne);
    inspector.visit("headerBuffer", headerBuffer);
    if (versionOne) {
      inspector.visit("firstValueBuffer", firstValueBuffer);
    } else {
      // Inspecting just one example of valueBuffer, not needed to inspect the whole array, because all buffers in it
      // are the same.
      inspector.visit("valueBuffer", valueBuffers.length > 0 ? valueBuffers[0] : null);
    }
    inspector.visit("strategy", strategy);
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

    T bufferedIndexedGet(ByteBuffer copyValueBuffer, int startOffset, int endOffset)
    {
      final int size = endOffset - startOffset;
      lastReadSize = size;
      if (size == 0) {
        return null;
      }
      // ObjectStrategy.fromByteBuffer() is allowed to reset the limit of the buffer. So if the limit is changed,
      // position() call in the next line could throw an exception, if the position is set beyond the new limit. clear()
      // sets the limit to the maximum possible, the capacity. It is safe to reset the limit to capacity, because the
      // value buffer(s) initial limit equals to capacity.
      copyValueBuffer.clear();
      copyValueBuffer.position(startOffset);
      return strategy.fromByteBuffer(copyValueBuffer, size);
    }

    /**
     * This method makes no guarantees with respect to thread safety
     *
     * @return the size in bytes of the last value read
     */
    int getLastValueSize()
    {
      return lastReadSize;
    }

    @Override
    public int indexOf(T value)
    {
      return GenericIndexed.this.indexOf(this, value);
    }

    @Override
    public Iterator<T> iterator()
    {
      return GenericIndexed.this.iterator();
    }
  }

  ///////////////
  // VERSION ONE
  ///////////////

  private static <T> GenericIndexed<T> createGenericIndexedVersionOne(ByteBuffer byteBuffer, ObjectStrategy<T> strategy)
  {
    boolean allowReverseLookup = byteBuffer.get() == REVERSE_LOOKUP_ALLOWED;
    int size = byteBuffer.getInt();
    ByteBuffer bufferToUse = byteBuffer.asReadOnlyBuffer();
    bufferToUse.limit(bufferToUse.position() + size);
    byteBuffer.position(bufferToUse.limit());

    return new GenericIndexed<>(
        bufferToUse,
        strategy,
        allowReverseLookup
    );
  }

  private static <T> GenericIndexed<T> fromIterableVersionOne(Iterable<T> objectsIterable, ObjectStrategy<T> strategy)
  {
    Iterator<T> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(Ints.BYTES).putInt(0);
      buffer.flip();
      return new GenericIndexed<>(buffer, strategy, true);
    }

    boolean allowReverseLookup = true;
    int count = 0;

    ZeroCopyByteArrayOutputStream headerBytes = new ZeroCopyByteArrayOutputStream();
    ZeroCopyByteArrayOutputStream valueBytes = new ZeroCopyByteArrayOutputStream();
    ByteBuffer helperBuffer = ByteBuffer.allocate(Ints.BYTES);
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
        offset += Ints.BYTES + bytes.length;
        SerializerUtils.writeBigEndianIntToOutputStream(headerBytes, offset, helperBuffer);
        SerializerUtils.writeBigEndianIntToOutputStream(valueBytes, bytes.length, helperBuffer);
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
    theBuffer.putInt(count);
    headerBytes.writeTo(theBuffer);
    valueBytes.writeTo(theBuffer);
    theBuffer.flip();

    return new GenericIndexed<>(theBuffer.asReadOnlyBuffer(), strategy, allowReverseLookup);
  }

  private long getSerializedSizeVersionOne()
  {
    return theBuffer.remaining()
           + 1 // version byte
           + 1 // allowReverseLookup flag
           + Ints.BYTES // numBytesUsed
           + Ints.BYTES; // numElements
  }

  private T getVersionOne(int index)
  {
    checkIndex(index);

    final int startOffset;
    final int endOffset;

    if (index == 0) {
      startOffset = Ints.BYTES;
      endOffset = headerBuffer.getInt(0);
    } else {
      int headerPosition = (index - 1) * Ints.BYTES;
      startOffset = headerBuffer.getInt(headerPosition) + Ints.BYTES;
      endOffset = headerBuffer.getInt(headerPosition + Ints.BYTES);
    }
    return copyBufferAndGet(firstValueBuffer, startOffset, endOffset);
  }

  private BufferIndexed singleThreadedVersionOne()
  {
    final ByteBuffer copyBuffer = firstValueBuffer.asReadOnlyBuffer();
    return new BufferIndexed()
    {
      @Override
      public T get(final int index)
      {
        checkIndex(index);

        final int startOffset;
        final int endOffset;

        if (index == 0) {
          startOffset = 4;
          endOffset = headerBuffer.getInt(0);
        } else {
          int headerPosition = (index - 1) * Ints.BYTES;
          startOffset = headerBuffer.getInt(headerPosition) + Ints.BYTES;
          endOffset = headerBuffer.getInt(headerPosition + Ints.BYTES);
        }
        return bufferedIndexedGet(copyBuffer, startOffset, endOffset);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("headerBuffer", headerBuffer);
        inspector.visit("copyBuffer", copyBuffer);
        inspector.visit("strategy", strategy);
      }
    };
  }

  private void writeToChannelVersionOne(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{
        VERSION_ONE,
        allowReverseLookup ? REVERSE_LOOKUP_ALLOWED : REVERSE_LOOKUP_DISALLOWED
    }));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(theBuffer.remaining() + Ints.BYTES)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
    channel.write(theBuffer.asReadOnlyBuffer());
  }


  ///////////////
  // VERSION TWO
  ///////////////

  private static <T> GenericIndexed<T> createGenericIndexedVersionTwo(
      ByteBuffer byteBuffer,
      ObjectStrategy<T> strategy,
      SmooshedFileMapper fileMapper
  )
  {
    if (fileMapper == null) {
      throw new IAE("SmooshedFileMapper can not be null for version 2.");
    }
    boolean allowReverseLookup = byteBuffer.get() == REVERSE_LOOKUP_ALLOWED;
    int logBaseTwoOfElementsPerValueFile = byteBuffer.getInt();
    int numElements = byteBuffer.getInt();

    try {
      String columnName = SERIALIZER_UTILS.readString(byteBuffer);
      int elementsPerValueFile = 1 << logBaseTwoOfElementsPerValueFile;
      int numberOfFilesRequired = getNumberOfFilesRequired(elementsPerValueFile, numElements);
      ByteBuffer[] valueBuffersToUse = new ByteBuffer[numberOfFilesRequired];
      for (int i = 0; i < numberOfFilesRequired; i++) {
        // SmooshedFileMapper.mapFile() contract guarantees that the valueBuffer's limit equals to capacity.
        ByteBuffer valueBuffer = fileMapper.mapFile(GenericIndexedWriter.generateValueFileName(columnName, i));
        valueBuffersToUse[i] = valueBuffer.asReadOnlyBuffer();
      }
      ByteBuffer headerBuffer = fileMapper.mapFile(GenericIndexedWriter.generateHeaderFileName(columnName));
      return new GenericIndexed<>(
          valueBuffersToUse,
          headerBuffer,
          strategy,
          allowReverseLookup,
          logBaseTwoOfElementsPerValueFile,
          numElements
      );
    }
    catch (IOException e) {
      throw new RuntimeException("File mapping failed.", e);
    }
  }

  private T getVersionTwo(int index)
  {
    checkIndex(index);

    final int startOffset;
    final int endOffset;

    int relativePositionOfIndex = index & relativeIndexMask;
    if (relativePositionOfIndex == 0) {
      int headerPosition = index * Ints.BYTES;
      startOffset = Ints.BYTES;
      endOffset = headerBuffer.getInt(headerPosition);
    } else {
      int headerPosition = (index - 1) * Ints.BYTES;
      startOffset = headerBuffer.getInt(headerPosition) + Ints.BYTES;
      endOffset = headerBuffer.getInt(headerPosition + Ints.BYTES);
    }
    int fileNum = index >> logBaseTwoOfElementsPerValueFile;
    return copyBufferAndGet(valueBuffers[fileNum], startOffset, endOffset);
  }

  private BufferIndexed singleThreadedVersionTwo()
  {
    final ByteBuffer[] copyValueBuffers = new ByteBuffer[valueBuffers.length];
    for (int i = 0; i < valueBuffers.length; i++) {
      copyValueBuffers[i] = valueBuffers[i].asReadOnlyBuffer();
    }

    return new BufferIndexed()
    {
      @Override
      public T get(final int index)
      {
        checkIndex(index);

        final int startOffset;
        final int endOffset;

        int relativePositionOfIndex = index & relativeIndexMask;
        if (relativePositionOfIndex == 0) {
          int headerPosition = index * Ints.BYTES;
          startOffset = 4;
          endOffset = headerBuffer.getInt(headerPosition);
        } else {
          int headerPosition = (index - 1) * Ints.BYTES;
          startOffset = headerBuffer.getInt(headerPosition) + Ints.BYTES;
          endOffset = headerBuffer.getInt(headerPosition + Ints.BYTES);
        }
        int fileNum = index >> logBaseTwoOfElementsPerValueFile;
        return bufferedIndexedGet(copyValueBuffers[fileNum], startOffset, endOffset);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("headerBuffer", headerBuffer);
        // Inspecting just one example of copyValueBuffer, not needed to inspect the whole array, because all buffers
        // in it are the same.
        inspector.visit("copyValueBuffer", copyValueBuffers.length > 0 ? copyValueBuffers[0] : null);
        inspector.visit("strategy", strategy);
      }
    };
  }
}
