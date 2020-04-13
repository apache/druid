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
 * Serializer that produces {@link BlockLayoutColumnarLongsSupplier.BlockLayoutColumnarLongs}.
 */
public class BlockLayoutColumnarLongsSerializer implements ColumnarLongsSerializer
{
  private static final MetaSerdeHelper<BlockLayoutColumnarLongsSerializer> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((BlockLayoutColumnarLongsSerializer x) -> CompressedColumnarLongsSupplier.VERSION)
      .writeInt(x -> x.numInserted)
      .writeInt(x -> x.sizePer)
      .writeSomething(CompressionFactory.longEncodingWriter(x -> x.writer, x -> x.compression));

  private final String columnName;
  private final int sizePer;
  private final CompressionFactory.LongEncodingWriter writer;
  private final GenericIndexedWriter<ByteBuffer> flattener;
  private final CompressionStrategy compression;
  private int numInserted = 0;
  private int numInsertedForNextFlush;

  @Nullable
  private ByteBuffer endBuffer;

  BlockLayoutColumnarLongsSerializer(
      String columnName,
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ByteOrder byteOrder,
      CompressionFactory.LongEncodingWriter writer,
      CompressionStrategy compression
  )
  {
    this.columnName = columnName;
    this.sizePer = writer.getBlockSize(CompressedPools.BUFFER_SIZE);
    int bufferSize = writer.getNumBytes(sizePer);
    this.flattener = GenericIndexedWriter.ofCompressedByteBuffers(segmentWriteOutMedium, filenameBase, compression, bufferSize);
    this.writer = writer;
    this.compression = compression;
    CompressionStrategy.Compressor compressor = compression.getCompressor();
    endBuffer = compressor.allocateInBuffer(writer.getNumBytes(sizePer), segmentWriteOutMedium.getCloser()).order(byteOrder);
    writer.setBuffer(endBuffer);
    numInsertedForNextFlush = sizePer;
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
  public void add(long value) throws IOException
  {
    if (endBuffer == null) {
      throw new IllegalStateException("written out already");
    }
    if (numInserted == numInsertedForNextFlush) {
      numInsertedForNextFlush += sizePer;
      writer.flush();
      endBuffer.flip();
      flattener.write(endBuffer);
      endBuffer.clear();
      writer.setBuffer(endBuffer);
    }

    writer.write(value);
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
      writer.flush();
      endBuffer.flip();
      if (endBuffer.remaining() > 0) {
        flattener.write(endBuffer);
      }
      endBuffer = null;
    }
  }
}
