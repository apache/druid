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

import io.druid.io.Channels;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.IndexIO;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Streams array of integers out in the binary format described by {@link V3CompressedVSizeColumnarMultiIntsSupplier}
 */
public class V3CompressedVSizeColumnarMultiIntsSerializer extends ColumnarMultiIntsSerializer
{
  private static final byte VERSION = V3CompressedVSizeColumnarMultiIntsSupplier.VERSION;

  public static V3CompressedVSizeColumnarMultiIntsSerializer create(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final String filenameBase,
      final int maxValue,
      final CompressionStrategy compression
  )
  {
    return new V3CompressedVSizeColumnarMultiIntsSerializer(
        new CompressedColumnarIntsSerializer(
            segmentWriteOutMedium,
            filenameBase,
            CompressedColumnarIntsSupplier.MAX_INTS_IN_BUFFER,
            IndexIO.BYTE_ORDER,
            compression
        ),
        new CompressedVSizeColumnarIntsSerializer(
            segmentWriteOutMedium,
            filenameBase,
            maxValue,
            CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue),
            IndexIO.BYTE_ORDER,
            compression
        )
    );
  }

  private final CompressedColumnarIntsSerializer offsetWriter;
  private final CompressedVSizeColumnarIntsSerializer valueWriter;
  private int offset;
  private boolean lastOffsetWritten = false;

  V3CompressedVSizeColumnarMultiIntsSerializer(
      CompressedColumnarIntsSerializer offsetWriter,
      CompressedVSizeColumnarIntsSerializer valueWriter
  )
  {
    this.offsetWriter = offsetWriter;
    this.valueWriter = valueWriter;
    this.offset = 0;
  }

  @Override
  public void open() throws IOException
  {
    offsetWriter.open();
    valueWriter.open();
  }

  @Override
  public void addValues(IndexedInts ints) throws IOException
  {
    if (lastOffsetWritten) {
      throw new IllegalStateException("written out already");
    }
    offsetWriter.addValue(offset);
    int numValues = ints.size();
    for (int i = 0; i < numValues; i++) {
      valueWriter.addValue(ints.get(i));
    }
    offset += numValues;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    writeLastOffset();
    return 1 + offsetWriter.getSerializedSize() + valueWriter.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeLastOffset();
    Channels.writeFully(channel, ByteBuffer.wrap(new byte[]{VERSION}));
    offsetWriter.writeTo(channel, smoosher);
    valueWriter.writeTo(channel, smoosher);
  }

  private void writeLastOffset() throws IOException
  {
    if (!lastOffsetWritten) {
      offsetWriter.addValue(offset);
      lastOffsetWritten = true;
    }
  }
}
