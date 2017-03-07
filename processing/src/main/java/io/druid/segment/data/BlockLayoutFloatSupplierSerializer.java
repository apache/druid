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

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.io.Channels;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.WritableByteChannel;

public class BlockLayoutFloatSupplierSerializer implements FloatSupplierSerializer
{
  private final int sizePer;
  private final GenericIndexedWriter<ResourceHolder<FloatBuffer>> flattener;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  private int numInserted = 0;
  private FloatBuffer endBuffer;

  BlockLayoutFloatSupplierSerializer(
      String filenameBase,
      ByteOrder order,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.sizePer = CompressedPools.BUFFER_SIZE / Floats.BYTES;
    this.flattener = new GenericIndexedWriter<>(
        filenameBase,
        CompressedFloatBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)
    );
    this.compression = compression;

    endBuffer = FloatBuffer.allocate(sizePer);
    endBuffer.mark();
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
    if (!endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer = FloatBuffer.allocate(sizePer);
      endBuffer.mark();
    }

    endBuffer.put(value);
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
    meta.put(CompressedFloatsIndexedSupplier.version);
    meta.putInt(numInserted);
    meta.putInt(sizePer);
    meta.put(compression.getId());
    meta.flip();

    Channels.writeFully(channel, meta);
    flattener.writeTo(channel, smoosher);
  }

  private void writeEndBuffer() throws IOException
  {
    if (endBuffer != null && numInserted > 0) {
      endBuffer.limit(endBuffer.position());
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer = null;
    }
  }

  private int metaSize()
  {
    return 1 + Ints.BYTES + Ints.BYTES + 1;
  }
}
