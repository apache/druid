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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


/**
 * {@link DictionaryWriter} for a {@link FrontCodedIndexed}, written to a {@link SegmentWriteOutMedium}.
 *
 * Front coding is a type of delta encoding for byte arrays, where values are grouped into buckets. The first value of
 * the bucket is written entirely, and remaining values are stored as pairs of an integer which indicates how much
 * of the first byte array of the bucket to use as a prefix, followed by the remaining value bytes after the prefix.
 */
public class FrontCodedIndexedWriter<T> implements DictionaryWriter<T>
{
  private static final int MAX_LOG_BUFFER_SIZE = 26;

  public static final FrontCoder<String> STRING_ENCODER = new StringFrontCoder();

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final int bucketSize;
  private final ByteOrder byteOrder;

  @Nullable
  private T prevObject = null;
  @Nullable
  private WriteOutBytes headerOut = null;
  @Nullable
  private WriteOutBytes valuesOut = null;
  private int numWritten = 0;

  private ByteBuffer scratch;
  private int logScratchSize = 10;
  private final List<T> bucketBuffer;
  private boolean isClosed = false;
  private boolean hasNulls = false;

  private final FrontCoder<T> frontCoder;

  public FrontCodedIndexedWriter(
      SegmentWriteOutMedium segmentWriteOutMedium,
      FrontCoder<T> frontCoder,
      ByteOrder byteOrder,
      int bucketSize
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.scratch = ByteBuffer.allocate(1 << logScratchSize).order(byteOrder);
    this.bucketSize = bucketSize;
    this.bucketBuffer = new ArrayList<>(bucketSize);
    for (int i = 0; i < bucketSize; i++) {
      bucketBuffer.add(null);
    }
    this.byteOrder = byteOrder;
    this.frontCoder = frontCoder;
  }

  @Override
  public void open() throws IOException
  {
    headerOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  public void write(@Nullable T value) throws IOException
  {
    final T objectToWrite = frontCoder.processValue(value);
    if (prevObject != null && frontCoder.compare(prevObject, objectToWrite) >= 0) {
      throw new ISE(
          "Values must be sorted and unique. Element [%s] with value [%s] is before or equivalent to [%s]",
          numWritten,
          objectToWrite,
          prevObject
      );
    }

    if (objectToWrite == null) {
      hasNulls = true;
      return;
    }

    if (numWritten > 0 && (numWritten % bucketSize) == 0) {
      resetScratch();
      int written;
      do {
        written = writeBucket(scratch, bucketBuffer, bucketSize, frontCoder);
        if (written < 0) {
          growScratch();
        }
      } while (written < 0);
      scratch.flip();
      Channels.writeFully(valuesOut, scratch);

      resetScratch();
      scratch.putInt((int) valuesOut.size());
      scratch.flip();
      Channels.writeFully(headerOut, scratch);
    }

    bucketBuffer.set(numWritten % bucketSize, objectToWrite);

    ++numWritten;
    prevObject = objectToWrite;
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
           VByte.estimateIntSize(numWritten) +
           VByte.estimateIntSize(headerAndValues) +
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

  private void flush() throws IOException
  {
    int remainder = numWritten % bucketSize;
    resetScratch();
    int written;
    do {
      written = writeBucket(scratch, bucketBuffer, remainder == 0 ? bucketSize : remainder, frontCoder);
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

  public static <T> int writeBucket(ByteBuffer buffer, List<T> values, int numValues, FrontCoder<T> frontCoder)
  {
    int written = 0;
    T first = null;
    while (written < numValues) {
      T next = values.get(written);
      if (written == 0) {
        first = next;
        int rem = writeValue(buffer, frontCoder.toBytes(first));
        if (rem < 0) {
          return rem;
        }
      } else {
        final FrontCoder.FrontCodedValue deltaEncoded = frontCoder.encodeValue(first, next);
        int rem = buffer.remaining() - VByte.estimateIntSize(deltaEncoded.getPrefixLength());
        if (rem < 0) {
          return rem;
        }
        VByte.writeInt(buffer, deltaEncoded.getPrefixLength());
        rem = writeValue(buffer, deltaEncoded.getSuffix());
        if (rem < 0) {
          return rem;
        }
      }
      written++;
    }
    return written;
  }

  public static int writeValue(ByteBuffer buffer, byte[] bytes)
  {
    final int remaining = buffer.remaining() - VByte.estimateIntSize(bytes.length) - bytes.length;
    if (remaining < 0) {
      return remaining;
    }
    final int pos = buffer.position();
    VByte.writeInt(buffer, bytes.length);
    buffer.put(bytes, 0, bytes.length);
    return buffer.position() - pos;
  }

  interface FrontCoder<T> extends Comparator<T>
  {
    @Nullable
    T processValue(@Nullable T value);

    byte[] toBytes(T value);
    FrontCodedValue encodeValue(T first, T value);

    class FrontCodedValue
    {
      private final int prefixLength;
      private final byte[] suffix;

      public FrontCodedValue(int prefixLength, byte[] suffix)
      {
        this.prefixLength = prefixLength;
        this.suffix = suffix;
      }

      public int getPrefixLength()
      {
        return prefixLength;
      }

      public byte[] getSuffix()
      {
        return suffix;
      }
    }
  }

  public static final class StringFrontCoder implements FrontCoder<String>
  {
    @Nullable
    @Override
    public String processValue(@Nullable String value)
    {
      return NullHandling.emptyToNullIfNeeded(value);
    }

    @Override
    public byte[] toBytes(String value)
    {
      return StringUtils.toUtf8(value);
    }

    @Override
    public FrontCodedValue encodeValue(String first, String next)
    {
      int i = 0;
      for (; i < first.length(); i++) {
        if (first.charAt(i) != next.charAt(i)) {
          break;
        }
      }
      return new FrontCodedValue(i, toBytes(next.substring(i)));
    }

    @Override
    public int compare(String o1, String o2)
    {
      return GenericIndexed.STRING_STRATEGY.compare(o1, o2);
    }
  }
}
