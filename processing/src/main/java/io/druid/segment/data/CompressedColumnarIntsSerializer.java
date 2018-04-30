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

import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.serde.MetaSerdeHelper;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Streams array of integers out in the binary format described by {@link CompressedColumnarIntsSupplier}
 */
public class CompressedColumnarIntsSerializer extends SingleValueColumnarIntsSerializer
{
  private static final byte VERSION = CompressedColumnarIntsSupplier.VERSION;

  private static final MetaSerdeHelper<CompressedColumnarIntsSerializer> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((CompressedColumnarIntsSerializer x) -> VERSION)
      .writeInt(x -> x.numInserted)
      .writeInt(x -> x.chunkFactor)
      .writeByte(x -> x.compression.getId());

  private final int chunkFactor;
  private final CompressionStrategy compression;
  private final GenericIndexedWriter<ByteBuffer> flattener;
  private ByteBuffer endBuffer;
  private int numInserted;

  CompressedColumnarIntsSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final String filenameBase,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressionStrategy compression
  )
  {
    this(
        segmentWriteOutMedium,
        chunkFactor,
        byteOrder,
        compression,
        GenericIndexedWriter.ofCompressedByteBuffers(
            segmentWriteOutMedium,
            filenameBase,
            compression,
            chunkFactor * Integer.BYTES
        )
    );
  }

  CompressedColumnarIntsSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressionStrategy compression,
      final GenericIndexedWriter<ByteBuffer> flattener
  )
  {
    this.chunkFactor = chunkFactor;
    this.compression = compression;
    this.flattener = flattener;
    CompressionStrategy.Compressor compressor = compression.getCompressor();
    Closer closer = segmentWriteOutMedium.getCloser();
    this.endBuffer = compressor.allocateInBuffer(chunkFactor * Integer.BYTES, closer).order(byteOrder);
    this.numInserted = 0;
  }

  @Override
  public void open() throws IOException
  {
    flattener.open();
  }

  @Override
  public void addValue(int val) throws IOException
  {
    if (endBuffer == null) {
      throw new IllegalStateException("written out already");
    }
    if (!endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(endBuffer);
      endBuffer.clear();
    }
    endBuffer.putInt(val);
    numInserted++;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    writeEndBuffer();
    return metaSerdeHelper.size(this) + flattener.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeEndBuffer();
    metaSerdeHelper.writeTo(channel, this);
    flattener.writeTo(channel, smoosher);
  }

  private void writeEndBuffer() throws IOException
  {
    if (endBuffer != null) {
      endBuffer.flip();
      if (endBuffer.remaining() > 0) {
        flattener.write(endBuffer);
      }
      endBuffer = null;
    }
  }
}
