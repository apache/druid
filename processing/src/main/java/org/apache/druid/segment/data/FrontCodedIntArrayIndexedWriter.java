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

import com.google.common.primitives.Ints;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Comparator;

public class FrontCodedIntArrayIndexedWriter implements DictionaryWriter<int[]>
{
  private static final int MAX_LOG_BUFFER_SIZE = 26;

  public static final Comparator<int[]> ARRAY_COMPARATOR = (o1, o2) -> {
    //noinspection ArrayEquality
    if (o1 == o2) {
      return 0;
    }
    if (o1 == null) {
      return -1;
    }
    if (o2 == null) {
      return 1;
    }
    final int iter = Math.min(o1.length, o2.length);
    for (int i = 0; i < iter; i++) {
      final int cmp = Integer.compare(o1[i], o2[i]);
      if (cmp == 0) {
        continue;
      }
      return cmp;
    }
    return Integer.compare(o1.length, o2.length);
  };

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final int bucketSize;
  private final ByteOrder byteOrder;
  private final int[][] bucketBuffer;
  private final ByteBuffer getOffsetBuffer;
  private final int div;

  @Nullable
  private int[] prevObject = null;
  @Nullable
  private WriteOutBytes headerOut = null;
  @Nullable
  private WriteOutBytes valuesOut = null;
  private int numWritten = 0;
  private ByteBuffer scratch;
  private int logScratchSize = 10;
  private boolean isClosed = false;
  private boolean hasNulls = false;

  public FrontCodedIntArrayIndexedWriter(
      SegmentWriteOutMedium segmentWriteOutMedium,
      ByteOrder byteOrder,
      int bucketSize
  )
  {
    if (Integer.bitCount(bucketSize) != 1 || bucketSize < 1 || bucketSize > 128) {
      throw new IAE("bucketSize must be a power of two (from 1 up to 128) but was[%,d]", bucketSize);
    }
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.scratch = ByteBuffer.allocate(1 << logScratchSize).order(byteOrder);
    this.bucketSize = bucketSize;
    this.byteOrder = byteOrder;
    this.bucketBuffer = new int[bucketSize][];
    this.getOffsetBuffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
    this.div = Integer.numberOfTrailingZeros(bucketSize);
  }

  @Override
  public void open() throws IOException
  {
    headerOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  public void write(@Nullable int[] value) throws IOException
  {

    if (prevObject != null && ARRAY_COMPARATOR.compare(prevObject, value) >= 0) {
      throw new ISE(
          "Values must be sorted and unique. Element [%s] with value [%s] is before or equivalent to [%s]",
          numWritten,
          value == null ? null : Arrays.toString(value),
          Arrays.toString(prevObject)
      );
    }

    if (value == null) {
      hasNulls = true;
      return;
    }

    // if the bucket buffer is full, write the bucket
    if (numWritten > 0 && (numWritten % bucketSize) == 0) {
      resetScratch();
      int written;
      // write the bucket, growing scratch buffer as necessary
      do {
        written = writeBucket(scratch, bucketBuffer, bucketSize);
        if (written < 0) {
          growScratch();
        }
      } while (written < 0);
      scratch.flip();
      Channels.writeFully(valuesOut, scratch);

      resetScratch();
      // write end offset for current value
      scratch.putInt((int) valuesOut.size());
      scratch.flip();
      Channels.writeFully(headerOut, scratch);
    }

    bucketBuffer[numWritten % bucketSize] = value;

    ++numWritten;
    prevObject = value;
  }


  @Override
  public long getSerializedSize() throws IOException
  {
    if (!isClosed) {
      flush();
    }
    int headerAndValues = Ints.checkedCast(headerOut.size() + valuesOut.size());
    return Byte.BYTES +
           Byte.BYTES +
           Byte.BYTES +
           VByte.computeIntSize(numWritten) +
           VByte.computeIntSize(headerAndValues) +
           headerAndValues;
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    if (!isClosed) {
      flush();
    }
    resetScratch();
    // version 0
    scratch.put((byte) 0);
    scratch.put((byte) bucketSize);
    scratch.put(hasNulls ? NullHandling.IS_NULL_BYTE : NullHandling.IS_NOT_NULL_BYTE);
    VByte.writeInt(scratch, numWritten);
    VByte.writeInt(scratch, Ints.checkedCast(headerOut.size() + valuesOut.size()));
    scratch.flip();
    Channels.writeFully(channel, scratch);
    headerOut.writeTo(channel);
    valuesOut.writeTo(channel);
  }

  @Override
  public boolean isSorted()
  {
    return true;
  }

  @Nullable
  @Override
  public int[] get(int index) throws IOException
  {
    if (index == 0 && hasNulls) {
      return null;
    }
    final int adjustedIndex = hasNulls ? index - 1 : index;
    final int relativeIndex = adjustedIndex % bucketSize;
    // check for current page
    if (adjustedIndex >= numWritten - bucketSize) {
      return bucketBuffer[relativeIndex];
    } else {
      final int bucket = adjustedIndex >> div;
      long startOffset;
      if (bucket == 0) {
        startOffset = 0;
      } else {
        startOffset = getBucketOffset(bucket - 1);
      }
      long endOffset = getBucketOffset(bucket);
      int currentBucketSize = Ints.checkedCast(endOffset - startOffset);
      if (currentBucketSize == 0) {
        return null;
      }
      final ByteBuffer bucketBuffer = ByteBuffer.allocate(currentBucketSize).order(byteOrder);
      valuesOut.readFully(startOffset, bucketBuffer);
      bucketBuffer.clear();
      return getFromBucket(bucketBuffer, relativeIndex);
    }
  }

  @Override
  public int getCardinality()
  {
    return numWritten + (hasNulls ? 1 : 0);
  }

  private long getBucketOffset(int index) throws IOException
  {
    getOffsetBuffer.clear();
    headerOut.readFully(index * (long) Integer.BYTES, getOffsetBuffer);
    return getOffsetBuffer.getInt(0);
  }

  private void flush() throws IOException
  {
    if (numWritten == 0) {
      return;
    }
    int remainder = numWritten % bucketSize;
    resetScratch();
    int written;
    do {
      written = writeBucket(scratch, bucketBuffer, remainder == 0 ? bucketSize : remainder);
      if (written < 0) {
        growScratch();
      }
    } while (written < 0);
    scratch.flip();
    Channels.writeFully(valuesOut, scratch);
    resetScratch();
    isClosed = true;
  }

  private void resetScratch()
  {
    scratch.position(0);
    scratch.limit(scratch.capacity());
  }

  private void growScratch()
  {
    if (logScratchSize < MAX_LOG_BUFFER_SIZE) {
      this.scratch = ByteBuffer.allocate(1 << ++logScratchSize).order(byteOrder);
    } else {
      throw new IllegalStateException("scratch buffer to big to write buckets");
    }
  }

  /**
   * Write bucket of values to a {@link ByteBuffer}. The first value is written completely, subsequent values are
   * written with an integer to indicate how much of the first value in the bucket is a prefix of the value, followed
   * by the remaining bytes of the value.
   *
   * Uses {@link VByte} encoded integers to indicate prefix length and value length.
   */
  public static int writeBucket(ByteBuffer buffer, int[][] values, int numValues)
  {
    int written = 0;
    int[] prev = null;
    while (written < numValues) {
      int[] next = values[written];
      if (written == 0) {
        prev = next;
        // the first value in the bucket is written completely as it is
        int rem = writeValue(buffer, prev);
        // wasn't enough room, bail out
        if (rem < 0) {
          return rem;
        }
      } else {
        // all other values must be partitioned into a prefix length and suffix bytes
        int prefixLength = 0;
        for (; prefixLength < prev.length; prefixLength++) {
          final int cmp = Integer.compare(prev[prefixLength], next[prefixLength]);
          if (cmp != 0) {
            break;
          }
        }
        // convert to bytes because not every char is a single byte
        final int[] suffix = new int[next.length - prefixLength];
        System.arraycopy(next, prefixLength, suffix, 0, suffix.length);
        int rem = buffer.remaining() - VByte.computeIntSize(prefixLength);
        // wasn't enough room, bail out
        if (rem < 0) {
          return rem;
        }
        VByte.writeInt(buffer, prefixLength);
        rem = writeValue(buffer, suffix);
        prev = next;
        // wasn't enough room, bail out
        if (rem < 0) {
          return rem;
        }
      }
      written++;
    }
    return written;
  }

  /**
   * Write a variable length int[] value to a {@link ByteBuffer}, storing the length as a {@link VByte} encoded
   * integer followed by the value itself. Returns the number of bytes written to the buffer. This method returns a
   * negative value if there is no room available in the buffer, so that it can be grown if needed.
   */
  public static int writeValue(ByteBuffer buffer, int[] ints)
  {
    int remaining = buffer.remaining() - VByte.computeIntSize(ints.length) - ints.length;
    if (remaining < 0) {
      return remaining;
    }
    final int pos = buffer.position();
    VByte.writeInt(buffer, ints.length);
    for (int anInt : ints) {
      remaining = buffer.remaining() - Integer.BYTES;
      if (remaining < 0) {
        return remaining;
      }
      buffer.putInt(anInt);
    }
    return buffer.position() - pos;
  }

  /**
   * Copy of {@link FrontCodedIntArrayIndexed#getFromBucket(ByteBuffer, int)} but with local declarations of arrays
   * for unwinding stuff
   */
  int[] getFromBucket(ByteBuffer buffer, int offset)
  {
    int[] unwindPrefixLength = new int[bucketSize];
    int[] unwindBufferPosition = new int[bucketSize];
    // first value is written whole
    final int length = VByte.readInt(buffer);
    if (offset == 0) {
      final int[] firstValue = new int[length];
      for (int i = 0; i < length; i++) {
        firstValue[i] = buffer.getInt();
      }
      return firstValue;
    }
    int pos = 0;
    int prefixLength;
    int fragmentLength;
    unwindPrefixLength[pos] = 0;
    unwindBufferPosition[pos] = buffer.position();

    buffer.position(buffer.position() + (length * Integer.BYTES));
    do {
      prefixLength = VByte.readInt(buffer);
      if (++pos < offset) {
        // not there yet, no need to read anything other than the length to skip ahead
        final int skipLength = VByte.readInt(buffer);
        unwindPrefixLength[pos] = prefixLength;
        unwindBufferPosition[pos] = buffer.position();
        buffer.position(buffer.position() + (skipLength * Integer.BYTES));
      } else {
        // we've reached our destination
        fragmentLength = VByte.readInt(buffer);
        if (prefixLength == 0) {
          // no prefix, return it directly
          final int[] value = new int[fragmentLength];
          for (int i = 0; i < fragmentLength; i++) {
            value[i] = buffer.getInt();
          }
          return value;
        }
        break;
      }
    } while (true);
    final int valueLength = prefixLength + fragmentLength;
    final int[] value = new int[valueLength];
    for (int i = prefixLength; i < valueLength; i++) {
      value[i] = buffer.getInt();
    }
    for (int i = prefixLength; i > 0;) {
      // previous value had a larger prefix than or the same as the value we are looking for
      // skip it since the fragment doesn't have anything we need
      if (unwindPrefixLength[--pos] >= i) {
        continue;
      }
      buffer.position(unwindBufferPosition[pos]);
      final int prevLength = unwindPrefixLength[pos];
      for (int fragmentOffset = 0; fragmentOffset < i - prevLength; fragmentOffset++) {
        value[prevLength + fragmentOffset] = buffer.getInt();
      }
      i = unwindPrefixLength[pos];
    }
    return value;
  }
}
