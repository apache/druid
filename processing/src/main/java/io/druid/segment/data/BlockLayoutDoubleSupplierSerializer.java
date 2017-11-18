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

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;


public class BlockLayoutDoubleSupplierSerializer implements DoubleSupplierSerializer
{
  private final IOPeon ioPeon;
  private final int sizePer;
  private final GenericIndexedWriter<ResourceHolder<DoubleBuffer>> flattener;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final String metaFile;

  private long metaCount = 0;
  private int numInserted = 0;
  private DoubleBuffer endBuffer;

  public BlockLayoutDoubleSupplierSerializer(
      IOPeon ioPeon,
      String filenameBase,
      ByteOrder order,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.ioPeon = ioPeon;
    this.sizePer = CompressedPools.BUFFER_SIZE / Doubles.BYTES;
    this.flattener = new GenericIndexedWriter<>(
        ioPeon, filenameBase, CompressedDoubleBufferObjectStrategy.getBufferForOrder(order, compression, sizePer)
    );
    this.metaFile = filenameBase + ".format";
    this.compression = compression;

    endBuffer = DoubleBuffer.allocate(sizePer);
    endBuffer.mark();
  }

  @Override
  public void open() throws IOException
  {
    flattener.open();
  }

  @Override
  public void add(double value) throws IOException
  {
    if (!endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer = DoubleBuffer.allocate(sizePer);
      endBuffer.mark();
    }

    endBuffer.put(value);
    ++numInserted;
  }

  @Override
  public long getSerializedSize()
  {
    return metaCount + flattener.getSerializedSize();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    try (InputStream meta = ioPeon.makeInputStream(metaFile)) {
      ByteStreams.copy(Channels.newChannel(meta), channel);
      flattener.writeToChannel(channel, smoosher);
    }
  }

  @Override
  public void close() throws IOException
  {
    endBuffer.limit(endBuffer.position());
    endBuffer.rewind();
    flattener.write(StupidResourceHolder.create(endBuffer));
    endBuffer = null;
    flattener.close();

    try (CountingOutputStream metaOut = new CountingOutputStream(ioPeon.makeOutputStream(metaFile))) {
      metaOut.write(CompressedDoublesIndexedSupplier.version);
      metaOut.write(Ints.toByteArray(numInserted));
      metaOut.write(Ints.toByteArray(sizePer));
      metaOut.write(compression.getId());
      metaOut.close();
      metaCount = metaOut.getCount();
    }
  }
}
