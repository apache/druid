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
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Streams array of integers out in the binary format described by CompressedIntsIndexedSupplier
 */
public class CompressedIntsIndexedWriter
{
  public static final byte version = CompressedIntsIndexedSupplier.version;

  private final int chunkFactor;
  private final ByteOrder byteOrder;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final GenericIndexedWriter<ResourceHolder<IntBuffer>> flattener;
  private IntBuffer endBuffer;
  private int numInserted;

  public CompressedIntsIndexedWriter(
      final IOPeon ioPeon,
      final String filenameBase,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.chunkFactor = chunkFactor;
    this.byteOrder = byteOrder;
    this.compression = compression;
    this.flattener = new GenericIndexedWriter<>(
        ioPeon, filenameBase, CompressedIntBufferObjectStrategy.getBufferForOrder(byteOrder, compression, chunkFactor)
    );
    this.endBuffer = IntBuffer.allocate(chunkFactor);
    this.numInserted = 0;
  }

  public void open() throws IOException
  {
    flattener.open();
  }

  public void add(int val) throws IOException
  {
    if (!endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer.rewind();
    }
    endBuffer.put(val);
    numInserted++;
  }

  public long closeAndWriteToChannel(WritableByteChannel channel) throws IOException
  {
    if (numInserted > 0) {
      endBuffer.limit(endBuffer.position());
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
    }
    endBuffer = null;
    flattener.close();

    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(numInserted)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(chunkFactor)));
    channel.write(ByteBuffer.wrap(new byte[]{compression.getId()}));
    final ReadableByteChannel from = Channels.newChannel(flattener.combineStreams().getInput());
    long dataLen = ByteStreams.copy(from, channel);
    return 1 +             // version
           Ints.BYTES +    // numInserted
           Ints.BYTES +    // chunkFactor
           1 +             // compression id
           dataLen;        // data
  }
}
