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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;


/**
 * {@link DictionaryWriter} for a {@link FrontCodedIndexed}, written to a {@link SegmentWriteOutMedium}. Values MUST
 * be added to this dictionary writer in sorted order, which is enforced.
 *
 * Front coding is a type of delta encoding for byte arrays, where values are grouped into buckets. The first value of
 * the bucket is written entirely, and remaining values are stored as pairs of an integer which indicates how much
 * of the first byte array of the bucket to use as a prefix, (or the preceding value of the bucket if using
 * 'incremental' buckets) followed by the remaining value bytes after the prefix.
 *
 * This writer is designed for use with UTF-8 encoded strings that are written in an order compatible with
 * {@link String#compareTo(String)}.
 *
 * @see FrontCodedIndexed for additional details.
 */
public class FrontCodedIndexedWriter implements DictionaryWriter<byte[]>
{
  private static final int MAX_LOG_BUFFER_SIZE = 26;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final int bucketSize;
  private final ByteOrder byteOrder;
  private final byte[][] bucketBuffer;
  private final ByteBuffer getOffsetBuffer;
  private final int div;
  private final byte version;

  @Nullable
  private byte[] prevObject = null;
  @Nullable
  private WriteOutBytes headerOut = null;
  @Nullable
  private WriteOutBytes valuesOut = null;
  private int numWritten = 0;
  private ByteBuffer scratch;
  private int logScratchSize = 10;
  private boolean isClosed = false;
  private boolean hasNulls = false;


  public FrontCodedIndexedWriter(
      SegmentWriteOutMedium segmentWriteOutMedium,
      ByteOrder byteOrder,
      int bucketSize,
      byte version
  )
  {
    if (Integer.bitCount(bucketSize) != 1 || bucketSize < 1 || bucketSize > 128) {
      throw new IAE("bucketSize must be a power of two (from 1 up to 128) but was[%,d]", bucketSize);
    }
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.scratch = ByteBuffer.allocate(1 << logScratchSize).order(byteOrder);
    this.bucketSize = bucketSize;
    this.byteOrder = byteOrder;
    this.bucketBuffer = new byte[bucketSize][];
    this.getOffsetBuffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
    this.div = Integer.numberOfTrailingZeros(bucketSize);
    this.version = FrontCodedIndexed.validateVersion(version);
  }

  @Override
  public void open() throws IOException
  {
    headerOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  public void write(@Nullable byte[] value) throws IOException
  {
    if (prevObject != null && compareNullableUtf8UsingJavaStringOrdering(prevObject, value) >= 0) {
      throw new ISE(
          "Values must be sorted and unique. Element [%s] with value [%s] is before or equivalent to [%s]",
          numWritten,
          value == null ? null : StringUtils.fromUtf8(value),
          StringUtils.fromUtf8(prevObject)
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
        written = version == FrontCodedIndexed.V1
                  ? writeBucketV1(scratch, bucketBuffer, bucketSize)
                  : writeBucketV0(scratch, bucketBuffer, bucketSize);
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
    scratch.put(version);
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
  public byte[] get(int index) throws IOException
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
      int bucketBytesSize = Ints.checkedCast(endOffset - startOffset);
      if (bucketBytesSize == 0) {
        return null;
      }
      final ByteBuffer bucketBuffer = ByteBuffer.allocate(bucketBytesSize).order(byteOrder);
      valuesOut.readFully(startOffset, bucketBuffer);
      bucketBuffer.clear();
      final ByteBuffer valueBuffer = version == FrontCodedIndexed.V1
                                     ? getFromBucketV1(bucketBuffer, relativeIndex, bucketSize)
                                     : FrontCodedIndexed.FrontCodedV0.getValueFromBucket(bucketBuffer, relativeIndex);
      final byte[] valueBytes = new byte[valueBuffer.limit() - valueBuffer.position()];
      valueBuffer.get(valueBytes);
      return valueBytes;
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
    getOffsetBuffer.clear();
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
      int flushSize = remainder == 0 ? bucketSize : remainder;
      written = version == FrontCodedIndexed.V1
                ? writeBucketV1(scratch, bucketBuffer, flushSize)
                : writeBucketV0(scratch, bucketBuffer, flushSize);
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
  public static int writeBucketV0(ByteBuffer buffer, byte[][] values, int numValues)
  {
    int written = 0;
    byte[] first = null;
    while (written < numValues) {
      byte[] next = values[written];
      if (written == 0) {
        first = next;
        // the first value in the bucket is written completely as it is
        int rem = writeValue(buffer, first);
        // wasn't enough room, bail out
        if (rem < 0) {
          return rem;
        }
      } else {
        // all other values must be partitioned into a prefix length and suffix bytes
        int prefixLength = 0;
        for (; prefixLength < first.length; prefixLength++) {
          final int cmp = StringUtils.compareUtf8UsingJavaStringOrdering(first[prefixLength], next[prefixLength]);
          if (cmp != 0) {
            break;
          }
        }
        // convert to bytes because not every char is a single byte
        final byte[] suffix = new byte[next.length - prefixLength];
        System.arraycopy(next, prefixLength, suffix, 0, suffix.length);
        int rem = buffer.remaining() - VByte.computeIntSize(prefixLength);
        // wasn't enough room, bail out
        if (rem < 0) {
          return rem;
        }
        VByte.writeInt(buffer, prefixLength);
        rem = writeValue(buffer, suffix);
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
   * Write bucket of values to a {@link ByteBuffer}. The first value is written completely, subsequent values are
   * written with an integer to indicate how much of the preceding value in the bucket is a prefix of the value,
   * followed by the remaining bytes of the value.
   *
   * Uses {@link VByte} encoded integers to indicate prefix length and value length.
   */
  public static int writeBucketV1(ByteBuffer buffer, byte[][] values, int numValues)
  {
    int written = 0;
    byte[] prev = null;
    while (written < numValues) {
      byte[] next = values[written];
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
          final int cmp = StringUtils.compareUtf8UsingJavaStringOrdering(prev[prefixLength], next[prefixLength]);
          if (cmp != 0) {
            break;
          }
        }
        // convert to bytes because not every char is a single byte
        final byte[] suffix = new byte[next.length - prefixLength];
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
   * Write a variable length byte[] value to a {@link ByteBuffer}, storing the length as a {@link VByte} encoded
   * integer followed by the value itself. Returns the number of bytes written to the buffer. This method returns a
   * negative value if there is no room available in the buffer, so that it can be grown if needed.
   */
  public static int writeValue(ByteBuffer buffer, byte[] bytes)
  {
    final int remaining = buffer.remaining() - VByte.computeIntSize(bytes.length) - bytes.length;
    if (remaining < 0) {
      return remaining;
    }
    final int pos = buffer.position();
    VByte.writeInt(buffer, bytes.length);
    buffer.put(bytes, 0, bytes.length);
    return buffer.position() - pos;
  }

  /**
   * Same as {@link StringUtils#compareUtf8UsingJavaStringOrdering(byte[], byte[])}, but accepts nulls. Nulls are
   * sorted first.
   */
  private static int compareNullableUtf8UsingJavaStringOrdering(
      @Nullable final byte[] b1,
      @Nullable final byte[] b2
  )
  {
    if (b1 == null) {
      return b2 == null ? 0 : -1;
    }

    if (b2 == null) {
      return 1;
    }

    return StringUtils.compareUtf8UsingJavaStringOrdering(b1, b2);
  }

  /**
   * same as {@link FrontCodedIndexed.FrontCodedV1#getFromBucket(ByteBuffer, int)} but
   * without re-using prefixLength and buffer position arrays so has more overhead/garbage creation than the instance
   * method.
   *
   * Note: adding the unwindPrefixLength and unwindBufferPosition arrays as arguments and having
   * {@link FrontCodedIndexed.FrontCodedV1#getFromBucket(ByteBuffer, int)} call this static method added 5-10ns of
   * overhead compared to having its own copy of the code, presumably due to the overhead of an additional method call
   * and extra arguments.
   *
   * As such, since the writer is the only user of this method, it has been copied here...
   */
  static ByteBuffer getFromBucketV1(ByteBuffer buffer, int offset, int bucketSize)
  {
    final int[] unwindPrefixLength = new int[bucketSize];
    final int[] unwindBufferPosition = new int[bucketSize];
    // first value is written whole
    final int length = VByte.readInt(buffer);
    if (offset == 0) {
      // return first value directly from underlying buffer since it is stored whole
      final ByteBuffer value = buffer.asReadOnlyBuffer();
      value.limit(value.position() + length);
      return value;
    }
    int pos = 0;
    int prefixLength;
    int fragmentLength;
    unwindPrefixLength[pos] = 0;
    unwindBufferPosition[pos] = buffer.position();

    buffer.position(buffer.position() + length);
    do {
      prefixLength = VByte.readInt(buffer);
      if (++pos < offset) {
        // not there yet, no need to read anything other than the length to skip ahead
        final int skipLength = VByte.readInt(buffer);
        unwindPrefixLength[pos] = prefixLength;
        unwindBufferPosition[pos] = buffer.position();
        buffer.position(buffer.position() + skipLength);
      } else {
        // we've reached our destination
        fragmentLength = VByte.readInt(buffer);
        if (prefixLength == 0) {
          // no prefix, return it directly from the underlying buffer
          final ByteBuffer value = buffer.asReadOnlyBuffer();
          value.limit(value.position() + fragmentLength);
          return value;
        }
        break;
      }
    } while (true);
    final int valueLength = prefixLength + fragmentLength;
    final byte[] valueBytes = new byte[valueLength];
    buffer.get(valueBytes, prefixLength, fragmentLength);
    for (int i = prefixLength; i > 0;) {
      // previous value had a larger prefix than or the same as the value we are looking for
      // skip it since the fragment doesn't have anything we need
      if (unwindPrefixLength[--pos] >= i) {
        continue;
      }
      buffer.position(unwindBufferPosition[pos]);
      buffer.get(valueBytes, unwindPrefixLength[pos], i - unwindPrefixLength[pos]);
      i = unwindPrefixLength[pos];
    }
    return ByteBuffer.wrap(valueBytes);
  }
}
