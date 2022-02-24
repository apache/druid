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
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * {@link DictionaryWriter} for a {@link FrontCodedIndexed}, written to a {@link SegmentWriteOutMedium}.
 *
 * Front coding is a type of delta encoding for strings, where values are grouped into buckets. The first value of
 * the bucket is written entirely, and remaining values are stored as pairs of an integer which indicates how much
 * of the first string of the bucket to use as a prefix, followed by the remaining string value after the prefix.
 */
public class FrontCodedIndexedWriter implements DictionaryWriter<String>
{
  private static final NullableTypeStrategy<String> NULLABLE_STRING_STRATEGY = ColumnType.STRING.getNullableStrategy();
  private static final int MAX_LOG_BUFFER_SIZE = 26;

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final int bucketSize;
  private final ByteOrder byteOrder;

  @Nullable
  private String prevObject = null;
  @Nullable
  private WriteOutBytes headerOut = null;
  @Nullable
  private WriteOutBytes valuesOut = null;
  private int numWritten = 0;

  private ByteBuffer scratch;
  private int logScratchSize = 10;
  private final String[] bucketBuffer;
  private boolean isClosed = false;
  private boolean hasNulls = false;

  public FrontCodedIndexedWriter(SegmentWriteOutMedium segmentWriteOutMedium, ByteOrder byteOrder, int bucketSize)
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.scratch = ByteBuffer.allocate(1 << logScratchSize).order(byteOrder);
    this.bucketSize = bucketSize;
    this.bucketBuffer = new String[bucketSize];
    this.byteOrder = byteOrder;
  }

  @Override
  public void open() throws IOException
  {
    headerOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  public void write(@Nullable String value) throws IOException
  {
    final String objectToWrite = NullHandling.emptyToNullIfNeeded(value);
    if (prevObject != null && NULLABLE_STRING_STRATEGY.compare(prevObject, objectToWrite) >= 0) {
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
        written = FrontCodedIndexed.writeBucket(scratch, bucketBuffer, bucketSize);
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

    bucketBuffer[numWritten % bucketSize] = objectToWrite;

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

  private void flush() throws IOException
  {
    int remainder = numWritten % bucketSize;
    resetScratch();
    int written;
    do {
      written = FrontCodedIndexed.writeBucket(scratch, bucketBuffer, remainder == 0 ? bucketSize : remainder);
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
}
