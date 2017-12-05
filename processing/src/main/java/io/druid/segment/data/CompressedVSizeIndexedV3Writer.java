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
import io.druid.segment.writeout.SegmentWriteOutMedium;
import io.druid.segment.CompressedVSizeIndexedV3Supplier;
import io.druid.segment.IndexIO;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Streams array of integers out in the binary format described by CompressedVSizeIndexedV3Supplier
 */
public class CompressedVSizeIndexedV3Writer extends MultiValueIndexedIntsWriter
{
  private static final byte VERSION = CompressedVSizeIndexedV3Supplier.VERSION;

  public static CompressedVSizeIndexedV3Writer create(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final String filenameBase,
      final int maxValue,
      final CompressionStrategy compression
  )
  {
    return new CompressedVSizeIndexedV3Writer(
        new CompressedIntsIndexedWriter(
            segmentWriteOutMedium,
            filenameBase,
            CompressedIntsIndexedSupplier.MAX_INTS_IN_BUFFER,
            IndexIO.BYTE_ORDER,
            compression
        ),
        new CompressedVSizeIntsIndexedWriter(
            segmentWriteOutMedium,
            filenameBase,
            maxValue,
            CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue),
            IndexIO.BYTE_ORDER,
            compression
        )
    );
  }

  private final CompressedIntsIndexedWriter offsetWriter;
  private final CompressedVSizeIntsIndexedWriter valueWriter;
  private int offset;
  private boolean lastOffsetWritten = false;

  CompressedVSizeIndexedV3Writer(
      CompressedIntsIndexedWriter offsetWriter,
      CompressedVSizeIntsIndexedWriter valueWriter
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
  protected void addValues(IntList vals) throws IOException
  {
    if (lastOffsetWritten) {
      throw new IllegalStateException("written out already");
    }
    if (vals == null) {
      vals = IntLists.EMPTY_LIST;
    }
    offsetWriter.add(offset);
    for (int i = 0; i < vals.size(); i++) {
      valueWriter.add(vals.getInt(i));
    }
    offset += vals.size();
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
      offsetWriter.add(offset);
      lastOffsetWritten = true;
    }
  }
}
