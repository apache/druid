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

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Serializer that produces {@link BlockLayoutColumnarFloatsSupplier.BlockLayoutColumnarFloats}.
 */
public class BlockLayoutColumnarFloatsSerializer implements ColumnarFloatsSerializer
{
  private static final MetaSerdeHelper<BlockLayoutColumnarFloatsSerializer> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((BlockLayoutColumnarFloatsSerializer x) -> CompressedColumnarFloatsSupplier.VERSION)
      .writeInt(x -> x.numInserted)
      .writeInt(x -> CompressedPools.BUFFER_SIZE / Float.BYTES)
      .writeByte(x -> x.compression.getId());

  private final String columnName;
  private final GenericIndexedWriter<ByteBuffer> flattener;
  private final CompressionStrategy compression;

  private int numInserted = 0;
  @Nullable
  private ByteBuffer endBuffer;

  BlockLayoutColumnarFloatsSerializer(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder byteOrder,
      CompressionStrategy compression
  )
  {
    this.columnName = columnName;
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
  public int size()
  {
    return numInserted;
  }

  @Override
  public void add(float value) throws IOException
  {
    if (endBuffer == null) {
      throw new IllegalStateException("written out already");
    }
    if (!endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(endBuffer);
      endBuffer.clear();
    }
    endBuffer.putFloat(value);
    ++numInserted;
    if (numInserted < 0) {
      throw new ColumnCapacityExceededException(columnName);
    }
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    writeEndBuffer();
    return META_SERDE_HELPER.size(this) + flattener.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeEndBuffer();
    META_SERDE_HELPER.writeTo(channel, this);
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
