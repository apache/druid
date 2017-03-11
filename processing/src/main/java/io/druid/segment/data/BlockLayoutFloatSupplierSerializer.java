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
import io.druid.io.Channels;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class BlockLayoutFloatSupplierSerializer implements FloatSupplierSerializer
{
  private final GenericIndexedWriter<ByteBuffer> flattener;
  private final CompressionStrategy compression;

  private int numInserted = 0;
  private ByteBuffer endBuffer;

  BlockLayoutFloatSupplierSerializer(
      String filenameBase,
      ByteOrder byteOrder,
      CompressionStrategy compression
  )
  {
    this.flattener = GenericIndexedWriter.ofCompressedByteBuffers(filenameBase, compression, CompressedPools.BUFFER_SIZE);
    this.compression = compression;
    this.endBuffer = compression.getCompressor().allocateInBuffer(CompressedPools.BUFFER_SIZE).order(byteOrder);
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
      endBuffer.clear();
      flattener.write(endBuffer);
    }
    endBuffer.putFloat(value);
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
    meta.putInt(CompressedPools.BUFFER_SIZE / Floats.BYTES);
    meta.put(compression.getId());
    meta.flip();

    Channels.writeFully(channel, meta);
    flattener.writeTo(channel, smoosher);
  }

  private void writeEndBuffer() throws IOException
  {
    if (endBuffer != null && numInserted > 0) {
      endBuffer.flip();
      flattener.write(endBuffer);
      endBuffer = null;
    }
  }

  private int metaSize()
  {
    return 1 + Ints.BYTES + Ints.BYTES + 1;
  }
}
