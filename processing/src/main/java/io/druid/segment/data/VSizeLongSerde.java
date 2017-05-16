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

import io.druid.java.util.common.IAE;

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

  public static final int SUPPORTED_SIZE[] = {1, 2, 4, 8, 12, 16, 20, 24, 32, 40, 48, 56, 64};
  public static final byte EMPTY[] = {0, 0, 0, 0};

  public static int getBitsForMax(long value)
  {
    if (value < 0) {
      throw new IAE("maxValue[%s] must be positive", value);
    }
    byte numBits = 0;
    long maxValue = 1;
    for (int i = 0; i < SUPPORTED_SIZE.length; i++) {
      while (numBits < SUPPORTED_SIZE[i] && maxValue < Long.MAX_VALUE / 2) {
        numBits++;
        maxValue *= 2;
      }
      if (value <= maxValue || maxValue >= Long.MAX_VALUE / 2) {
        return SUPPORTED_SIZE[i];
      }
    }
    return 64;
  }

  public static int getSerializedSize(int bitsPerValue, int numValues)
  {
    // this value is calculated by rounding up the byte and adding the 4 closing bytes
    return (bitsPerValue * numValues + 7) / 8 + 4;
  }

  // block size should be power of 2 so get of indexedLong can be optimized using bit operators
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
    OutputStream output = null;
    ByteBuffer buffer;
    byte curByte = 0;
    int count = 0;

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
      buffer.put((byte) (curByte << (8 - count)));
      if (output != null) {
        output.write(buffer.array());
        output.write(EMPTY);
        output.flush();
      } else {
        buffer.putInt(0);
      }
    }
  }

  private static final class Size2Ser implements LongSerializer
  {
    OutputStream output = null;
    ByteBuffer buffer;
    byte curByte = 0;
    int count = 0;

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
      buffer.put((byte) (curByte << (8 - count)));
      if (output != null) {
        output.write(buffer.array());
        output.write(EMPTY);
        output.flush();
      } else {
        buffer.putInt(0);
      }
    }
  }

  private static final class Mult4Ser implements LongSerializer
  {

    OutputStream output = null;
    ByteBuffer buffer;
    int numBytes;
    byte curByte = 0;
    boolean first = true;

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
    }
  }

  private static final class Mult8Ser implements LongSerializer
  {
    OutputStream output;
    ByteBuffer buffer;
    int numBytes;

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
      if (output != null) {
        output.write(EMPTY);
        output.flush();
      } else {
        buffer.putInt(0);
      }
    }
  }

  public interface LongDeserializer
  {
    long get(int index);
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
      int offset = (index * 3) >> 1;
      return (buffer.getShort(this.offset + offset) >> shift) & 0xFFF;
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
      int offset = (index * 5) >> 1;
      return (buffer.getInt(this.offset + offset) >> shift) & 0xFFFFF;
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
      return buffer.getInt(offset + index * 3) >>> 8;
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
      return buffer.getInt(offset + (index << 2)) & 0xFFFFFFFFL;
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
      return buffer.getLong(offset + index * 5) >>> 24;
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
      return buffer.getLong(offset + index * 6) >>> 16;
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
      return buffer.getLong(offset + index * 7) >>> 8;
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
  }

}
