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

package org.apache.druid.spectator.histogram;

import com.google.common.base.Preconditions;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.BitSet;
import java.util.Objects;

/**
 * A header for storing offsets for columns with nullable values.
 * Provides fast access to the offset start/end for a given row index, while supporting null values.
 * For cases where data is sparse, this can save a lot of space.
 * The nulls are stored in a bitset, and the offsets are stored in an int array.
 * The cost of the nulls is 1 bit per row, the cost of the non-nulls is 4 bytes per row for the offset.
 * In cases where every row is non-null, the bitset is omitted.
 * In either case, we need the offsets because the values are variable length.
 */
public class NullableOffsetsHeader implements Serializer
{
  private final WriteOutBytes offsetsWriter;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final BitSet valueBitmap;
  private int size = 0;
  private final IntBuffer offsetsReader;
  private final ByteBuffer bitsetBuffer;
  private final int[] cumlCardinality;
  private final int cardinality;

  private static final int CUML_COUNT_SIZE = Long.SIZE;
  private static final int CUML_COUNT_BYTES = Long.BYTES;

  public static NullableOffsetsHeader read(ByteBuffer buffer)
  {
    // Size + BitmapLength + ValueBitMap + Offsets
    final int size = buffer.getInt();
    final int bitmapLength = buffer.getInt();
    final int offsetPosition = buffer.position() + bitmapLength;

    // Grab the bitset
    final ByteBuffer bitsetBuffer = buffer.slice();
    bitsetBuffer.limit(bitmapLength);

    int[] cumlCardinality = null;
    int cardinality = 0;
    if (bitmapLength >= CUML_COUNT_BYTES) {
      // Create a quick lookup of the cumulative count of set bits up to
      // a given int index in the bitset. This is used to quickly get to
      // near the offset that we want.
      // Tradeoff is memory use vs scanning per get() call.
      LongBuffer bitBuffer = bitsetBuffer.asLongBuffer();
      cumlCardinality = new int[bitBuffer.limit()];
      int i = 0;

      while (bitBuffer.hasRemaining()) {
        long bits = bitBuffer.get();
        cardinality += Long.bitCount(bits);
        cumlCardinality[i++] = cardinality;
      }

      // Count any bits in the remaining bytes after the end of the 64-bit chunks
      // In cases where bitsetBuffer length doesn't directly divide into 64
      // there will be up to 7 bytes remaining, with at least 1 bit set somewhere
      // else the bytes would have been omitted.
      // We use cardinality to compute where offsets end, so the full count is important.
      int baseByteIndex = i * (CUML_COUNT_SIZE / Byte.SIZE);
      for (int byteIndex = baseByteIndex; byteIndex < bitsetBuffer.limit(); byteIndex++) {
        // Read the bit set for this byte within the 64 bits that need counting.
        int bitset = bitsetBuffer.get(byteIndex) & 0xFF;
        cardinality += BYTE_CARDINALITY[bitset];
      }
    } else if (bitmapLength > 0) {
      while (bitsetBuffer.hasRemaining()) {
        int bitset = bitsetBuffer.get() & 0xFF;
        cardinality += BYTE_CARDINALITY[bitset];
      }
    } else if (buffer.hasRemaining()) {
      // The header is "full", so the bitmap was omitted.
      // We'll have an offset per entry.
      cardinality = size;
    }

    // Grab the offsets
    buffer.position(offsetPosition);
    final int offsetsLength = cardinality * Integer.BYTES;
    final ByteBuffer offsetsBuffer = buffer.slice();
    offsetsBuffer.limit(offsetsLength);

    // Set the buffer position to after the offsets
    // to mark this whole header as "read"
    buffer.position(offsetPosition + offsetsLength);

    return new NullableOffsetsHeader(size, bitsetBuffer, cardinality, cumlCardinality, offsetsBuffer);
  }

  public static NullableOffsetsHeader create(SegmentWriteOutMedium segmentWriteOutMedium) throws IOException
  {
    Preconditions.checkNotNull(segmentWriteOutMedium, "segmentWriteOutMedium");
    return new NullableOffsetsHeader(segmentWriteOutMedium);
  }

  // Constructor for reading
  private NullableOffsetsHeader(int size, ByteBuffer bitset, int cardinality, int[] cumlCardinality, ByteBuffer offsetsBuffer)
  {
    this.segmentWriteOutMedium = null;
    this.offsetsWriter = null;
    this.valueBitmap = null;

    this.size = size;
    this.offsetsReader = offsetsBuffer.asIntBuffer();
    this.bitsetBuffer = bitset;
    this.cumlCardinality = cumlCardinality;
    this.cardinality = cardinality;
  }

  // Constructor for writing
  private NullableOffsetsHeader(SegmentWriteOutMedium segmentWriteOutMedium) throws IOException
  {
    this.offsetsReader = null;
    this.cumlCardinality = null;
    this.cardinality = 0;
    this.bitsetBuffer = null;

    this.valueBitmap = new BitSet();
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.offsetsWriter = this.segmentWriteOutMedium.makeWriteOutBytes();
  }

  public int size()
  {
    return size;
  }

  public int getCardinality()
  {
    return cardinality;
  }

  private void checkWriteable()
  {
    if (valueBitmap == null) {
      throw new NullPointerException("Write during deserialization");
    }
  }

  private void checkReadable()
  {
    if (offsetsReader == null) {
      throw new NullPointerException("Read during serialization");
    }
  }

  public void writeNull()
  {
    checkWriteable();

    // Nothing to write, but we need to "store" the null
    size++;
  }

  public void writeOffset(int offset) throws IOException
  {
    checkWriteable();

    int index = size++;
    valueBitmap.set(index);
    offsetsWriter.writeInt(offset);
  }

  @Override
  public long getSerializedSize()
  {
    checkWriteable();

    // Size + BitmapLength + ValueBitMap + Offsets
    int sizeField = Integer.BYTES;
    int bitmapLength = Integer.BYTES;
    // if all values are set, we omit the bitmap, so bytes taken by the bitmap is zero
    // bitset.length returns the highest bit index that's set.
    // i.e. the length in bits. Round up to the nearest byte.
    int valueBitMap = (size == valueBitmap.cardinality()) ? 0 : (valueBitmap.length() + 7) / 8;
    int offsetSize = valueBitmap.cardinality() * Integer.BYTES;
    return sizeField + bitmapLength + valueBitMap + offsetSize;
  }

  @Override
  public void writeTo(WritableByteChannel channel, @Nullable FileSmoosher smoosher) throws IOException
  {
    checkWriteable();

    // Size + BitmapLength + ValueBitMap + Offsets
    ByteBuffer headerBytes = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES);

    // Size
    headerBytes.putInt(size);

    // BitmapLength
    byte[] bytes = null;

    // Omit bitmap if all entries are set
    if (size == valueBitmap.cardinality()) {
      headerBytes.putInt(0);
    } else {
      bytes = valueBitmap.toByteArray();
      headerBytes.putInt(bytes.length);
    }

    // Write the size and length
    headerBytes.flip();
    Channels.writeFully(channel, headerBytes);

    // Write the ValueBitmap
    if (bytes != null) {
      Channels.writeFully(channel, ByteBuffer.wrap(bytes));
    }

    // Write the Offsets
    offsetsWriter.writeTo(channel);
  }

  @Nullable
  public Offset get(int index)
  {
    checkReadable();

    // Return null for any out of range indexes
    if (this.cardinality == 0 || index < 0 || index >= this.size) {
      return null;
    }

    // Find the index to the offset for this row index
    int offsetIndex = getOffsetIndex(index);
    if (offsetIndex < 0) {
      return null;
    }

    // Special case for the first entry
    if (offsetIndex == 0) {
      return new Offset(0, this.offsetsReader.get(0));
    }

    return new Offset(this.offsetsReader.get(offsetIndex - 1), this.offsetsReader.get(offsetIndex));
  }

  // Exposed for testing
  int getOffsetIndex(int index)
  {
    if (this.cardinality == this.size) {
      // If "full" return index
      return index;
    }

    // Bitset omits trailing nulls, so if index is off the end it's a null.
    final int bytePos = index / Byte.SIZE;
    if (bytePos >= this.bitsetBuffer.limit()) {
      return -1;
    }

    final int indexByte = this.bitsetBuffer.get(bytePos) & 0xFF;
    // Check for null, is our bit is set.
    if ((indexByte & (1 << index % Byte.SIZE)) == 0) {
      return -1;
    }

    // Get the cardinality for the (index/CUML_COUNT_SIZE)th entry.
    // Use that to jump to that point in the bitset to add any incremental bit counts
    // until we get to index.
    // That is then the index position of the offset in the offsets buffer.
    final int baseInt = index / CUML_COUNT_SIZE;
    int baseByteIndex = baseInt * (CUML_COUNT_SIZE / Byte.SIZE);
    int offsetIndex = baseInt == 0 ? 0 : this.cumlCardinality[baseInt - 1];

    // We always need to count the bits in the byte containing our index.
    // So do that here, then go back and fill in the counts for the
    // bytes between baseByteIndex and bytePos.
    // We need to mask out only the bits up to and including our index
    // to avoid counting later bits.
    int mask = (1 << index - (bytePos * Byte.SIZE)) - 1;
    int byteCardinality = BYTE_CARDINALITY[indexByte & mask];
    offsetIndex += byteCardinality;

    // After getting the cumulative cardinality upto the 64 bit boundary immediately
    // preceeding the 64 bits that contains our index, we need to accumulate the
    // cardinality up to the byte including our index.
    for (int byteIndex = baseByteIndex; byteIndex < bytePos; byteIndex++) {
      // Read the bit set for this byte within the 64 bits that need counting.
      int bitset = this.bitsetBuffer.get(byteIndex) & 0xFF;
      offsetIndex += BYTE_CARDINALITY[bitset];
    }

    return offsetIndex;
  }

  public static class Offset
  {
    private final int start;
    private final int end;

    Offset(int start, int end)
    {
      this.start = start;
      this.end = end;
    }

    int getStart()
    {
      return start;
    }

    int getEnd()
    {
      return end;
    }

    int getLength()
    {
      return end - start;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Offset offset = (Offset) o;
      return start == offset.start && end == offset.end;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(start, end);
    }
  }

  // The count of bits in a byte, keyed by the byte value itself
  private static final int[] BYTE_CARDINALITY = {
      0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
      1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
      1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
      1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
      3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
      1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
      3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
      3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
      3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
      4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
  };
}
