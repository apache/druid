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

import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.io.Channels;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class BlockLayoutLongSupplierSerializer implements LongSupplierSerializer
{
  private final int sizePer;
  private final CompressionFactory.LongEncodingWriter writer;
  private final GenericIndexedWriter<ResourceHolder<ByteBuffer>> flattener;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private int numInserted = 0;

  private ByteBuffer endBuffer = null;

  BlockLayoutLongSupplierSerializer(
      String filenameBase,
      ByteOrder order,
      CompressionFactory.LongEncodingWriter writer,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.sizePer = writer.getBlockSize(CompressedPools.BUFFER_SIZE);
    this.flattener = new GenericIndexedWriter<>(
        filenameBase,
        VSizeCompressedObjectStrategy.getBufferForOrder(
            order,
            compression,
            writer.getNumBytes(sizePer)
        )
    );
    this.writer = writer;
    this.compression = compression;
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
    if (numInserted % sizePer == 0) {
      if (endBuffer != null) {
        writer.flush();
        endBuffer.limit(endBuffer.position());
        endBuffer.rewind();
        flattener.write(StupidResourceHolder.create(endBuffer));
      }
      endBuffer = ByteBuffer.allocate(writer.getNumBytes(sizePer));
      writer.setBuffer(endBuffer);
    }

    writer.write(value);
    ++numInserted;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    writeEndBuffer();
    return metaSize() + flattener.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeEndBuffer();

    ByteBuffer meta = ByteBuffer.allocate(metaSize());
    meta.put(CompressedLongsIndexedSupplier.version);
    meta.putInt(numInserted);
    meta.putInt(sizePer);
    writer.putMeta(meta, compression);
    meta.flip();

    Channels.writeFully(channel, meta);
    flattener.writeTo(channel, smoosher);
  }

  private void writeEndBuffer() throws IOException
  {
    if (endBuffer != null && numInserted > 0) {
      writer.flush();
      endBuffer.limit(endBuffer.position());
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer = null;
    }
  }

  private int metaSize()
  {
    return 1 + Ints.BYTES + Ints.BYTES + writer.metaSize();
  }
}
