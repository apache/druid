/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.data;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.UOE;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Currently only support big endian
 * <p>
 * An empty 4 bytes is written upon closing to avoid index out of bound exception for deserializers that shift bytes
 */
public class VSizeLongSerde
{

  public static final int[] SUPPORTED_SIZES = {1, 2, 4, 8, 12, 16, 20, 24, 32, 40, 48, 56, 64};
  public static final byte[] EMPTY = {0, 0, 0, 0};

  public static int getBitsForMax(long value)
  {
    if (value < 0) {
      throw new IAE("maxValue[%s] must be positive", value);
    }
    byte numBits = 0;
    long maxValue = 1;
    for (int supportedSize : SUPPORTED_SIZES) {
      while (numBits < supportedSize && maxValue < Long.MAX_VALUE / 2) {
        numBits++;
        maxValue *= 2;
      }
      if (value <= maxValue || maxValue >= Long.MAX_VALUE / 2) {
        return supportedSize;
      }
    }
    return 64;
  }

  public static int getSerializedSize(int bitsPerValue, int numValues)
  {
    // this value is calculated by rounding up the byte and adding the 4 closing bytes
    return (bitsPerValue * numValues + 7) / 8 + 4;
  }

  /**
   * Block size should be power of 2, so {@link ColumnarLongs#get(int)} can be optimized using bit operators.
   */
  public static int getNumValuesPerBlock(int bitsPerValue, int blockSize)
  {
    int ret = 1;
    while (getSerializedSize(bitsPerValue, ret) <= blockSize) {
      ret *= 2;
    }
    return ret / 2;
  }

  public static LongSerializer getSerializer(int longSize, OutputStream output)
  {
    switch (longSize) {
      case 1:
        return new Size1Ser(output);
      case 2:
        return new Size2Ser(output);
      case 4:
        return new Mult4Ser(output, 0);
      case 8:
        return new Mult8Ser(output, 1);
      case 12:
        return new Mult4Ser(output, 1);
      case 16:
        return new Mult8Ser(output, 2);
      case 20:
        return new Mult4Ser(output, 2);
      case 24:
        return new Mult8Ser(output, 3);
      case 32:
        return new Mult8Ser(output, 4);
      case 40:
        return new Mult8Ser(output, 5);
      case 48:
        return new Mult8Ser(output, 6);
      case 56:
        return new Mult8Ser(output, 7);
      case 64:
        return new Mult8Ser(output, 8);
      default:
        throw new IAE("Unsupported size %s", longSize);
    }
  }

  public static LongSerializer getSerializer(int longSize, ByteBuffer buffer, int bufferOffset)
  {
    switch (longSize) {
      case 1:
        return new Size1Ser(buffer, bufferOffset);
      case 2:
        return new Size2Ser(buffer, bufferOffset);
      case 4:
        return new Mult4Ser(buffer, bufferOffset, 0);
      case 8:
        return new Mult8Ser(buffer, bufferOffset, 1);
      case 12:
        return new Mult4Ser(buffer, bufferOffset, 1);
      case 16:
        return new Mult8Ser(buffer, bufferOffset, 2);
      case 20:
        return new Mult4Ser(buffer, bufferOffset, 2);
      case 24:
        return new Mult8Ser(buffer, bufferOffset, 3);
      case 32:
        return new Mult8Ser(buffer, bufferOffset, 4);
      case 40:
        return new Mult8Ser(buffer, bufferOffset, 5);
      case 48:
        return new Mult8Ser(buffer, bufferOffset, 6);
      case 56:
        return new Mult8Ser(buffer, bufferOffset, 7);
      case 64:
        return new Mult8Ser(buffer, bufferOffset, 8);
      default:
        throw new IAE("Unsupported size %s", longSize);
    }
  }


  // LongDeserializers were adapted from Apache Lucene DirectReader, see:
  // https://github.com/apache/lucene-solr/blob/master/lucene/core/src/java/org/apache/lucene/util/packed/DirectReader.java
  public static LongDeserializer getDeserializer(int longSize, ByteBuffer fromBuffer, int bufferOffset)
  {
    // The buffer needs to be duplicated since the byte order is changed
    ByteBuffer buffer = fromBuffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    switch (longSize) {
      case 1:
        return new Size1Des(buffer, bufferOffset);
      case 2:
        return new Size2Des(buffer, bufferOffset);
      case 4:
        return new Size4Des(buffer, bufferOffset);
      case 8:
        return new Size8Des(buffer, bufferOffset);
      case 12:
        return new Size12Des(buffer, bufferOffset);
      case 16:
        return new Size16Des(buffer, bufferOffset);
      case 20:
        return new Size20Des(buffer, bufferOffset);
      case 24:
        return new Size24Des(buffer, bufferOffset);
      case 32:
        return new Size32Des(buffer, bufferOffset);
      case 40:
        return new Size40Des(buffer, bufferOffset);
      case 48:
        return new Size48Des(buffer, bufferOffset);
      case 56:
        return new Size56Des(buffer, bufferOffset);
      case 64:
        return new Size64Des(buffer, bufferOffset);
      default:
        throw new IAE("Unsupported size %s", longSize);
    }
  }

  public interface LongSerializer extends Closeable
  {
    void write(long value) throws IOException;
  }

  private static final class Size1Ser implements LongSerializer
  {
    @Nullable
    OutputStream output = null;
    ByteBuffer buffer;
    byte curByte = 0;
    int count = 0;
    private boolean closed = false;

    public Size1Ser(OutputStream output)
    {
      this.output = output;
      this.buffer = ByteBuffer.allocate(1);
    }

    public Size1Ser(ByteBuffer buffer, int offset)
    {
      this.buffer = buffer;
      this.buffer.position(offset);
    }

    @Override
    public void write(long value) throws IOException
    {
      Preconditions.checkArgument(value >= 0);
      if (count == 8) {
        buffer.put(curByte);
        count = 0;
        if (!buffer.hasRemaining() && output != null) {
          output.write(buffer.array());
          buffer.rewind();
        }
      }
      curByte = (byte) ((curByte << 1) | (value & 1));
      count++;
    }

    @Override
    public void close() throws IOException
    {
      if (closed) {
        return;
      }
      buffer.put((byte) (curByte << (8 - count)));
      if (output != null) {
        output.write(buffer.array());
        output.write(EMPTY);
        output.flush();
      } else {
        buffer.putInt(0);
      }
      closed = true;
    }
  }

  private static final class Size2Ser implements LongSerializer
  {
    @Nullable
    OutputStream output = null;
    ByteBuffer buffer;
    byte curByte = 0;
    int count = 0;
    private boolean closed = false;

    public Size2Ser(OutputStream output)
    {
      this.output = output;
      this.buffer = ByteBuffer.allocate(1);
    }

    public Size2Ser(ByteBuffer buffer, int offset)
    {
      this.buffer = buffer;
      this.buffer.position(offset);
    }

    @Override
    public void write(long value) throws IOException
    {
      Preconditions.checkArgument(value >= 0);
      if (count == 8) {
        buffer.put(curByte);
        count = 0;
        if (!buffer.hasRemaining() && output != null) {
          output.write(buffer.array());
          buffer.rewind();
        }
      }
      curByte = (byte) ((curByte << 2) | (value & 3));
      count += 2;
    }

    @Override
    public void close() throws IOException
    {
      if (closed) {
        return;
      }
      buffer.put((byte) (curByte << (8 - count)));
      if (output != null) {
        output.write(buffer.array());
        output.write(EMPTY);
        output.flush();
      } else {
        buffer.putInt(0);
      }
      closed = true;
    }
  }

  private static final class Mult4Ser implements LongSerializer
  {
    @Nullable
    OutputStream output;
    ByteBuffer buffer;
    int numBytes;
    byte curByte = 0;
    boolean first = true;
    private boolean closed = false;

    public Mult4Ser(OutputStream output, int numBytes)
    {
      this.output = output;
      this.buffer = ByteBuffer.allocate(numBytes * 2 + 1);
      this.numBytes = numBytes;
    }

    public Mult4Ser(ByteBuffer buffer, int offset, int numBytes)
    {
      this.buffer = buffer;
      this.buffer.position(offset);
      this.numBytes = numBytes;
    }

    @Override
    public void write(long value) throws IOException
    {
      Preconditions.checkArgument(value >= 0);
      int shift = 0;
      if (first) {
        shift = 4;
        curByte = (byte) value;
        first = false;
      } else {
        curByte = (byte) ((curByte << 4) | ((value >> (numBytes << 3)) & 0xF));
        buffer.put(curByte);
        first = true;
      }
      for (int i = numBytes - 1; i >= 0; i--) {
        buffer.put((byte) (value >>> (i * 8 + shift)));
      }
      if (!buffer.hasRemaining() && output != null) {
        output.write(buffer.array());
        buffer.rewind();
      }
    }

    @Override
    public void close() throws IOException
    {
      if (closed) {
        return;
      }
      if (!first) {
        buffer.put((byte) (curByte << 4));
      }
      if (output != null) {
        output.write(buffer.array(), 0, buffer.position());
        output.write(EMPTY);
        output.flush();
      } else {
        buffer.putInt(0);
      }
      closed = true;
    }
  }

  private static final class Mult8Ser implements LongSerializer
  {
    @Nullable
    OutputStream output;
    ByteBuffer buffer;
    int numBytes;
    private boolean closed = false;

    public Mult8Ser(OutputStream output, int numBytes)
    {
      this.output = output;
      this.buffer = ByteBuffer.allocate(1);
      this.numBytes = numBytes;
    }

    public Mult8Ser(ByteBuffer buffer, int offset, int numBytes)
    {
      this.buffer = buffer;
      this.buffer.position(offset);
      this.numBytes = numBytes;
    }

    @Override
    public void write(long value) throws IOException
    {
      if (numBytes != 8) {
        // if the value is not stored in a full long, ensure it is zero or positive
        Preconditions.checkArgument(value >= 0);
      }
      for (int i = numBytes - 1; i >= 0; i--) {
        buffer.put((byte) (value >>> (i * 8)));
        if (output != null) {
          output.write(buffer.array());
          buffer.position(0);
        }
      }
    }

    @Override
    public void close() throws IOException
    {
      if (closed) {
        return;
      }
      if (output != null) {
        output.write(EMPTY);
        output.flush();
      } else {
        buffer.putInt(0);
      }
      closed = true;
    }
  }

  /**
   * Unpack bitpacked long values from an underlying contiguous memory block
   */
  public interface LongDeserializer
  {
    /**
     * Unpack long value at the specified row index
     */
    long get(int index);

    /**
     * Unpack a contiguous vector of long values at the specified start index of length and adjust them by the supplied
     * delta base value.
     */
    void getDelta(long[] out, int outPosition, int startIndex, int length, long base);

    /**
     * Unpack a non-contiguous vector of long values at the specified indexes and adjust them by the supplied delta base
     * value.
     */
    default int getDelta(long[] out, int outPosition, int[] indexes, int length, int indexOffset, int limit, long base)
    {
      for (int i = 0; i < length; i++) {
        int index = indexes[outPosition + i] - indexOffset;
        if (index >= limit) {
          return i;
        }

        out[outPosition + i] = base + get(index);
      }

      return length;
    }

    /**
     * Unpack a contiguous vector of long values at the specified start index of length and lookup and replace stored
     * values based on their index in the supplied value lookup 'table'
     */
    default void getTable(long[] out, int outPosition, int startIndex, int length, long[] table)
    {
      throw new UOE("Table decoding not supported for %s", this.getClass().getSimpleName());
    }

    /**
     * Unpack a contiguous vector of long values at the specified indexes and lookup and replace stored values based on
     * their index in the supplied value lookup 'table'
     */
    default int getTable(
        long[] out,
        int outPosition,
        int[] indexes,
        int length,
        int indexOffset,
        int limit,
        long[] table
    )
    {
      for (int i = 0; i < length; i++) {
        int index = indexes[outPosition + i] - indexOffset;
        if (index >= limit) {
          return i;
        }

        out[outPosition + i] = table[(int) get(index)];
      }

      return length;
    }
  }

  private static final class Size1Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size1Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      int shift = 7 - (index & 7);
      return (buffer.get(offset + (index >> 3)) >> shift) & 1;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x7) != 0 && i < length) {
        out[outPosition + i++] = base + get(index++);
      }
      for ( ; i + Byte.SIZE < length; index += Byte.SIZE) {
        final byte unpack = buffer.get(offset + (index >> 3));
        out[outPosition + i++] = base + (unpack >> 7) & 1;
        out[outPosition + i++] = base + (unpack >> 6) & 1;
        out[outPosition + i++] = base + (unpack >> 5) & 1;
        out[outPosition + i++] = base + (unpack >> 4) & 1;
        out[outPosition + i++] = base + (unpack >> 3) & 1;
        out[outPosition + i++] = base + (unpack >> 2) & 1;
        out[outPosition + i++] = base + (unpack >> 1) & 1;
        out[outPosition + i++] = base + unpack & 1;
      }
      while (i < length) {
        out[outPosition + i++] = base + get(index++);
      }
    }

    @Override
    public void getTable(long[] out, int outPosition, int startIndex, int length, long[] table)
    {
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x7) != 0 && i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
      for ( ; i + Byte.SIZE < length; index += Byte.SIZE) {
        final byte unpack = buffer.get(offset + (index >> 3));
        out[outPosition + i++] = table[(unpack >> 7) & 1];
        out[outPosition + i++] = table[(unpack >> 6) & 1];
        out[outPosition + i++] = table[(unpack >> 5) & 1];
        out[outPosition + i++] = table[(unpack >> 4) & 1];
        out[outPosition + i++] = table[(unpack >> 3) & 1];
        out[outPosition + i++] = table[(unpack >> 2) & 1];
        out[outPosition + i++] = table[(unpack >> 1) & 1];
        out[outPosition + i++] = table[unpack & 1];
      }
      while (i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
    }
  }

  private static final class Size2Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size2Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      int shift = 6 - ((index & 3) << 1);
      return (buffer.get(offset + (index >> 2)) >> shift) & 3;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x3) != 0 && i < length) {
        out[outPosition + i++] = base + get(index++);
      }
      for ( ; i + 8 < length; index += 8) {
        final short unpack = buffer.getShort(offset + (index >> 2));
        out[outPosition + i++] = base + (unpack >> 14) & 3;
        out[outPosition + i++] = base + (unpack >> 12) & 3;
        out[outPosition + i++] = base + (unpack >> 10) & 3;
        out[outPosition + i++] = base + (unpack >> 8) & 3;
        out[outPosition + i++] = base + (unpack >> 6) & 3;
        out[outPosition + i++] = base + (unpack >> 4) & 3;
        out[outPosition + i++] = base + (unpack >> 2) & 3;
        out[outPosition + i++] = base + unpack & 3;
      }
      while (i < length) {
        out[outPosition + i++] = base + get(index++);
      }
    }

    @Override
    public void getTable(long[] out, int outPosition, int startIndex, int length, long[] table)
    {
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x3) != 0 && i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
      for ( ; i + 8 < length; index += 8) {
        final short unpack = buffer.getShort(offset + (index >> 2));
        out[outPosition + i++] = table[(unpack >> 14) & 3];
        out[outPosition + i++] = table[(unpack >> 12) & 3];
        out[outPosition + i++] = table[(unpack >> 10) & 3];
        out[outPosition + i++] = table[(unpack >> 8) & 3];
        out[outPosition + i++] = table[(unpack >> 6) & 3];
        out[outPosition + i++] = table[(unpack >> 4) & 3];
        out[outPosition + i++] = table[(unpack >> 2) & 3];
        out[outPosition + i++] = table[unpack & 3];
      }
      while (i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
    }
  }

  private static final class Size4Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size4Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      int shift = ((index + 1) & 1) << 2;
      return (buffer.get(offset + (index >> 1)) >> shift) & 0xF;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x1) != 0 && i < length) {
        out[outPosition + i++] = base + get(index++) & 0xF;
      }
      for ( ; i + 8 < length; index += 8) {
        final int unpack = buffer.getInt(offset + (index >> 1));
        out[outPosition + i++] = base + (unpack >> 28) & 0xF;
        out[outPosition + i++] = base + (unpack >> 24) & 0xF;
        out[outPosition + i++] = base + (unpack >> 20) & 0xF;
        out[outPosition + i++] = base + (unpack >> 16) & 0xF;
        out[outPosition + i++] = base + (unpack >> 12) & 0xF;
        out[outPosition + i++] = base + (unpack >> 8) & 0xF;
        out[outPosition + i++] = base + (unpack >> 4) & 0xF;
        out[outPosition + i++] = base + unpack & 0xF;
      }
      while (i < length) {
        out[outPosition + i++] = base + get(index++);
      }
    }

    @Override
    public void getTable(long[] out, int outPosition, int startIndex, int length, long[] table)
    {
      int index = startIndex;
      int i = 0;

      // byte align
      while ((index & 0x1) != 0 && i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
      for ( ; i + 8 < length; index += 8) {
        final int unpack = buffer.getInt(offset + (index >> 1));
        out[outPosition + i++] = table[(unpack >> 28) & 0xF];
        out[outPosition + i++] = table[(unpack >> 24) & 0xF];
        out[outPosition + i++] = table[(unpack >> 20) & 0xF];
        out[outPosition + i++] = table[(unpack >> 16) & 0xF];
        out[outPosition + i++] = table[(unpack >> 12) & 0xF];
        out[outPosition + i++] = table[(unpack >> 8) & 0xF];
        out[outPosition + i++] = table[(unpack >> 4) & 0xF];
        out[outPosition + i++] = table[unpack & 0xF];
      }
      while (i < length) {
        out[outPosition + i++] = table[(int) get(index++)];
      }
    }
  }

  private static final class Size8Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size8Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      return buffer.get(offset + index) & 0xFF;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      for (int i = 0, indexOffset = startIndex; i < length; i++, indexOffset++) {
        out[outPosition + i] = base + buffer.get(offset + indexOffset) & 0xFF;
      }
    }

    @Override
    public int getDelta(long[] out, int outPosition, int[] indexes, int length, int indexOffset, int limit, long base)
    {
      for (int i = 0; i < length; i++) {
        int index = indexes[outPosition + i] - indexOffset;
        if (index >= limit) {
          return i;
        }

        out[outPosition + i] = base + (buffer.get(offset + index) & 0xFF);
      }

      return length;
    }

    @Override
    public void getTable(long[] out, int outPosition, int startIndex, int length, long[] table)
    {
      for (int i = 0, indexOffset = startIndex; i < length; i++, indexOffset++) {
        out[outPosition + i] = table[buffer.get(offset + indexOffset) & 0xFF];
      }
    }

    @Override
    public int getTable(long[] out, int outPosition, int[] indexes, int length, int indexOffset, int limit, long[] table)
    {
      for (int i = 0; i < length; i++) {
        int index = indexes[outPosition + i] - indexOffset;
        if (index >= limit) {
          return i;
        }

        out[outPosition + i] = table[buffer.get(offset + index) & 0xFF];
      }

      return length;
    }
  }

  private static final class Size12Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size12Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      int shift = ((index + 1) & 1) << 2;
      int indexOffset = (index * 3) >> 1;
      return (buffer.getShort(offset + indexOffset) >> shift) & 0xFFF;
    }


    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int i = 0;
      int index = startIndex;
      // every other value is byte aligned
      if ((index & 0x1) != 0) {
        out[outPosition + i++] = get(index++);
      }
      final int unpackSize = Long.BYTES + Integer.BYTES;
      for (int indexOffset = (index * 3) >> 1; i + 8 < length; indexOffset += unpackSize) {
        final long unpack = buffer.getLong(offset + indexOffset);
        final int unpack2 = buffer.getInt(offset + indexOffset + Long.BYTES);
        out[outPosition + i++] = base + ((unpack >> 52) & 0xFFF);
        out[outPosition + i++] = base + ((unpack >> 40) & 0xFFF);
        out[outPosition + i++] = base + ((unpack >> 28) & 0xFFF);
        out[outPosition + i++] = base + ((unpack >> 16) & 0xFFF);
        out[outPosition + i++] = base + ((unpack >> 4) & 0xFFF);
        out[outPosition + i++] = base + (((unpack & 0xF) << 8) | ((unpack2 >>> 24) & 0xFF));
        out[outPosition + i++] = base + ((unpack2 >> 12) & 0xFFF);
        out[outPosition + i++] = base + (unpack2 & 0xFFF);
      }
      while (i < length) {
        out[outPosition + i] = base + get(startIndex + i);
        i++;
      }
    }
  }

  private static final class Size16Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size16Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      return buffer.getShort(offset + (index << 1)) & 0xFFFF;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      for (int i = 0, indexOffset = (startIndex << 1); i < length; i++, indexOffset += Short.BYTES) {
        out[outPosition + i] = base + buffer.getShort(offset + indexOffset) & 0xFFFF;
      }
    }

    @Override
    public int getDelta(long[] out, int outPosition, int[] indexes, int length, int indexOffset, int limit, long base)
    {
      for (int i = 0; i < length; i++) {
        int index = indexes[outPosition + i] - indexOffset;
        if (index >= limit) {
          return i;
        }

        out[outPosition + i] = base + buffer.getShort(offset + (index << 1)) & 0xFFFF;
      }

      return length;
    }
  }

  private static final class Size20Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size20Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      int shift = (((index + 1) & 1) << 2) + 8;
      int indexOffset = (index * 5) >> 1;
      return (buffer.getInt(offset + indexOffset) >> shift) & 0xFFFFF;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int i = 0;
      int index = startIndex;
      // every other value is byte aligned
      if ((index & 0x1) != 0) {
        out[outPosition + i++] = get(index++);
      }
      final int unpackSize = Long.BYTES + Long.BYTES + Integer.BYTES;
      for (int indexOffset = (index * 5) >> 1; i + 8 < length; indexOffset += unpackSize) {
        final long unpack = buffer.getLong(offset + indexOffset);
        final long unpack2 = buffer.getLong(offset + indexOffset + Long.BYTES);
        final int unpack3 = buffer.getInt(offset + indexOffset + Long.BYTES + Long.BYTES);
        out[outPosition + i++] = base + ((unpack >> 44) & 0xFFFFF);
        out[outPosition + i++] = base + ((unpack >> 24) & 0xFFFFF);
        out[outPosition + i++] = base + ((unpack >> 4) & 0xFFFFF);
        out[outPosition + i++] = base + (((unpack & 0xF) << 16) | ((unpack2 >>> 48) & 0xFFFF));
        out[outPosition + i++] = base + ((unpack2 >> 28) & 0xFFFFF);
        out[outPosition + i++] = base + ((unpack2 >> 8) & 0xFFFFF);
        out[outPosition + i++] = base + (((unpack2 & 0xFF) << 12) | ((unpack3 >>> 20) & 0xFFF));
        out[outPosition + i++] = base + (unpack3 & 0xFFFFF);
      }
      while (i < length) {
        out[outPosition + i] = base + get(startIndex + i);
        i++;
      }
    }
  }

  private static final class Size24Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size24Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      return buffer.getInt(offset + (index * 3)) >>> 8;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int i = 0;
      final int unpackSize = 3 * Long.BYTES;
      for (int indexOffset = startIndex * 3; i + 8 < length; indexOffset += unpackSize) {
        final long unpack = buffer.getLong(offset + indexOffset);
        final long unpack2 = buffer.getLong(offset + indexOffset + Long.BYTES);
        final long unpack3 = buffer.getLong(offset + indexOffset + Long.BYTES + Long.BYTES);
        out[outPosition + i++] = base + ((unpack >> 40) & 0xFFFFFF);
        out[outPosition + i++] = base + ((unpack >> 16) & 0xFFFFFF);
        out[outPosition + i++] = base + (((unpack & 0xFFFF) << 8) | ((unpack2 >>> 56) & 0xFF));
        out[outPosition + i++] = base + ((unpack2 >> 32) & 0xFFFFFF);
        out[outPosition + i++] = base + ((unpack2 >> 8) & 0xFFFFFF);
        out[outPosition + i++] = base + (((unpack2 & 0xFF) << 16) | ((unpack3 >>> 48) & 0xFFFF));
        out[outPosition + i++] = base + ((unpack3 >> 24) & 0xFFFFFF);
        out[outPosition + i++] = base + (unpack3 & 0xFFFFFF);
      }
      while (i < length) {
        out[outPosition + i] = base + get(startIndex + i);
        i++;
      }
    }
  }

  private static final class Size32Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size32Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      return buffer.getInt((offset + (index << 2))) & 0xFFFFFFFFL;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      for (int i = 0, indexOffset = (startIndex << 2); i < length; i++, indexOffset += Integer.BYTES) {
        out[outPosition + i] = base + buffer.getInt(offset + indexOffset) & 0xFFFFFFFFL;
      }
    }
  }

  private static final class Size40Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size40Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      return buffer.getLong(offset + (index * 5)) >>> 24;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int i = 0;
      final int unpackSize = 5 * Long.BYTES;
      for (int indexOffset = startIndex * 5; i + 8 < length; indexOffset += unpackSize) {
        final long unpack = buffer.getLong(offset + indexOffset);
        final long unpack2 = buffer.getLong(offset + indexOffset + Long.BYTES);
        final long unpack3 = buffer.getLong(offset + indexOffset + (2 * Long.BYTES));
        final long unpack4 = buffer.getLong(offset + indexOffset + (3 * Long.BYTES));
        final long unpack5 = buffer.getLong(offset + indexOffset + (4 * Long.BYTES));
        out[outPosition + i++] = base + ((unpack >>> 24) & 0xFFFFFFFFFFL);
        out[outPosition + i++] = base + (((unpack & 0xFFFFFFL) << 16) | ((unpack2 >>> 48) & 0xFFFFL));
        out[outPosition + i++] = base + ((unpack2 >>> 8) & 0xFFFFFFFFFFL);
        out[outPosition + i++] = base + (((unpack2 & 0xFFL) << 32) | ((unpack3 >>> 32) & 0xFFFFFFFFL));
        out[outPosition + i++] = base + (((unpack3 & 0xFFFFFFFFL) << 8) | ((unpack4 >>> 56) & 0xFFL));
        out[outPosition + i++] = base + ((unpack4 >>> 16) & 0xFFFFFFFFFFL);
        out[outPosition + i++] = base + (((unpack4 & 0xFFFFL) << 24) | ((unpack5 >>> 40) & 0xFFFFFFL));
        out[outPosition + i++] = base + (unpack5 & 0xFFFFFFFFFFL);
      }
      while (i < length) {
        out[outPosition + i] = base + get(startIndex + i);
        i++;
      }
    }
  }

  private static final class Size48Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size48Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      return buffer.getLong(offset + (index * 6)) >>> 16;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int i = 0;
      final int unpackSize = 6 * Long.BYTES;
      for (int indexOffset = startIndex * 6; i + 8 < length; indexOffset += unpackSize) {
        final long unpack = buffer.getLong(offset + indexOffset);
        final long unpack2 = buffer.getLong(offset + indexOffset + Long.BYTES);
        final long unpack3 = buffer.getLong(offset + indexOffset + (2 * Long.BYTES));
        final long unpack4 = buffer.getLong(offset + indexOffset + (3 * Long.BYTES));
        final long unpack5 = buffer.getLong(offset + indexOffset + (4 * Long.BYTES));
        final long unpack6 = buffer.getLong(offset + indexOffset + (5 * Long.BYTES));
        out[outPosition + i++] = base + ((unpack >>> 16) & 0xFFFFFFFFFFFFL);
        out[outPosition + i++] = base + (((unpack & 0xFFFFL) << 32) | ((unpack2 >>> 32) & 0xFFFFFFFFL));
        out[outPosition + i++] = base + (((unpack2 & 0xFFFFFFFFL) << 16) | ((unpack3 >>> 48) & 0xFFFFL));
        out[outPosition + i++] = base + (unpack3 & 0xFFFFFFFFFFFFL);
        out[outPosition + i++] = base + ((unpack4 >>> 16) & 0xFFFFFFFFFFFFL);
        out[outPosition + i++] = base + (((unpack4 & 0xFFFFL) << 32) | ((unpack5 >>> 32) & 0xFFFFFFFFL));
        out[outPosition + i++] = base + (((unpack5 & 0xFFFFFFFFL) << 16) | ((unpack6 >>> 48) & 0xFFFFL));
        out[outPosition + i++] = base + (unpack6 & 0xFFFFFFFFFFFFL);
      }
      while (i < length) {
        out[outPosition + i] = base + get(startIndex + i);
        i++;
      }
    }
  }

  private static final class Size56Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size56Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      return buffer.getLong(offset + (index * 7)) >>> 8;
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      int i = 0;
      final int unpackSize = 7 * Long.BYTES;
      for (int indexOffset = startIndex * 7; i + 8 < length; indexOffset += unpackSize) {
        final long unpack = buffer.getLong(offset + indexOffset);
        final long unpack2 = buffer.getLong(offset + indexOffset + Long.BYTES);
        final long unpack3 = buffer.getLong(offset + indexOffset + (2 * Long.BYTES));
        final long unpack4 = buffer.getLong(offset + indexOffset + (3 * Long.BYTES));
        final long unpack5 = buffer.getLong(offset + indexOffset + (4 * Long.BYTES));
        final long unpack6 = buffer.getLong(offset + indexOffset + (5 * Long.BYTES));
        final long unpack7 = buffer.getLong(offset + indexOffset + (6 * Long.BYTES));
        out[outPosition + i++] = base + ((unpack >>> 8) & 0xFFFFFFFFFFFFFFL);
        out[outPosition + i++] = base + (((unpack & 0xFFL) << 48) | ((unpack2 >>> 16) & 0xFFFFFFFFFFFFL));
        out[outPosition + i++] = base + (((unpack2 & 0xFFFFL) << 40) | ((unpack3 >>> 24) & 0xFFFFFFFFFFL));
        out[outPosition + i++] = base + (((unpack3 & 0xFFFFFFL) << 32) | ((unpack4 >>> 32) & 0xFFFFFFFFL));
        out[outPosition + i++] = base + (((unpack4 & 0xFFFFFFFFL) << 24) | ((unpack5 >>> 40) & 0xFFFFFFL));
        out[outPosition + i++] = base + (((unpack5 & 0xFFFFFFFFFFL) << 16) | ((unpack6 >>> 48) & 0xFFFFL));
        out[outPosition + i++] = base + (((unpack6 & 0xFFFFFFFFFFFFL) << 8) | ((unpack7 >>> 56) & 0xFFL));
        out[outPosition + i++] = base + (unpack7 & 0xFFFFFFFFFFFFFFL);
      }
      while (i < length) {
        out[outPosition + i] = base + get(startIndex + i);
        i++;
      }
    }
  }

  private static final class Size64Des implements LongDeserializer
  {
    final ByteBuffer buffer;
    final int offset;

    public Size64Des(ByteBuffer buffer, int bufferOffset)
    {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }

    @Override
    public long get(int index)
    {
      return buffer.getLong(offset + (index << 3));
    }

    @Override
    public void getDelta(long[] out, int outPosition, int startIndex, int length, long base)
    {
      for (int i = 0, indexOffset = (startIndex << 3); i < length; i++, indexOffset += Long.BYTES) {
        out[outPosition + i] = base + buffer.getLong(offset + indexOffset);
      }
    }

    @Override
    public int getDelta(long[] out, int outPosition, int[] indexes, int length, int indexOffset, int limit, long base)
    {
      for (int i = 0; i < length; i++) {
        int index = indexes[outPosition + i] - indexOffset;
        if (index >= limit) {
          return i;
        }
        out[outPosition + i] = base + buffer.getLong(offset + (index << 3));
      }
      return length;
    }
  }
}
