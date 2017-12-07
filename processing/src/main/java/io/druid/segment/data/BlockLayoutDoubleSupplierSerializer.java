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
import io.druid.segment.CompressedPools;
import io.druid.segment.serde.MetaSerdeHelper;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;


public class BlockLayoutDoubleSupplierSerializer implements DoubleSupplierSerializer
{
  private static final MetaSerdeHelper<BlockLayoutDoubleSupplierSerializer> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((BlockLayoutDoubleSupplierSerializer x) -> CompressedDoublesIndexedSupplier.VERSION)
      .writeInt(x -> x.numInserted)
      .writeInt(x -> CompressedPools.BUFFER_SIZE / Double.BYTES)
      .writeByte(x -> x.compression.getId());

  private final GenericIndexedWriter<ByteBuffer> flattener;
  private final CompressionStrategy compression;

  private int numInserted = 0;
  private ByteBuffer endBuffer;

  BlockLayoutDoubleSupplierSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder byteOrder,
      CompressionStrategy compression
  )
  {
    this.flattener = GenericIndexedWriter.ofCompressedByteBuffers(
        segmentWriteOutMedium,
        filenameBase,
        compression,
        CompressedPools.BUFFER_SIZE
    );
    this.compression = compression;
    CompressionStrategy.Compressor compressor = compression.getCompressor();
    Closer closer = segmentWriteOutMedium.getCloser();
    this.endBuffer = compressor.allocateInBuffer(CompressedPools.BUFFER_SIZE, closer).order(byteOrder);
  }

  @Override
  public void open() throws IOException
  {
    flattener.open();
  }

  @Override
  public void add(double value) throws IOException
  {
    if (endBuffer == null) {
      throw new IllegalStateException("written out already");
    }
    if (!endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(endBuffer);
      endBuffer.clear();
    }

    endBuffer.putDouble(value);
    ++numInserted;
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
