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
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.io.Channels;
import io.druid.segment.IndexIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Streams array of integers out in the binary format described by CompressedVSizeIntsIndexedSupplier
 */
public class CompressedVSizeIntsIndexedWriter extends SingleValueIndexedIntsWriter
{
  private static final byte VERSION = CompressedVSizeIntsIndexedSupplier.VERSION;

  public static CompressedVSizeIntsIndexedWriter create(
      final String filenameBase,
      final int maxValue,
      final CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    return new CompressedVSizeIntsIndexedWriter(
        filenameBase,
        maxValue,
        CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(maxValue),
        IndexIO.BYTE_ORDER,
        compression
    );
  }

  private final int numBytes;
  private final int chunkFactor;
  private final int chunkBytes;
  private final ByteOrder byteOrder;
  private final CompressedObjectStrategy.CompressionStrategy compression;
  private final GenericIndexedWriter<ResourceHolder<ByteBuffer>> flattener;
  private final ByteBuffer intBuffer;

  private ByteBuffer endBuffer;
  private int numInserted;

  CompressedVSizeIntsIndexedWriter(
      final String filenameBase,
      final int maxValue,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this(
        maxValue,
        chunkFactor,
        byteOrder,
        compression,
        new GenericIndexedWriter<>(
            filenameBase,
            CompressedByteBufferObjectStrategy.getBufferForOrder(
                byteOrder,
                compression,
                sizePer(maxValue, chunkFactor)
            )
        )
    );
  }

  public CompressedVSizeIntsIndexedWriter(
      final int maxValue,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressedObjectStrategy.CompressionStrategy compression,
      final GenericIndexedWriter flattener
  )
  {
    this.numBytes = VSizeIndexedInts.getNumBytesForMax(maxValue);
    this.chunkFactor = chunkFactor;
    this.chunkBytes = chunkFactor * numBytes + CompressedVSizeIntsIndexedSupplier.bufferPadding(numBytes);
    this.byteOrder = byteOrder;
    this.compression = compression;
    this.flattener = flattener;
    this.intBuffer = ByteBuffer.allocate(Ints.BYTES).order(byteOrder);
    this.endBuffer = ByteBuffer.allocate(chunkBytes).order(byteOrder);
    this.endBuffer.limit(numBytes * chunkFactor);
    this.numInserted = 0;
  }

  private static int sizePer(int maxValue, int chunkFactor)
  {
    return chunkFactor * VSizeIndexedInts.getNumBytesForMax(maxValue)
           + CompressedVSizeIntsIndexedSupplier.bufferPadding(VSizeIndexedInts.getNumBytesForMax(maxValue));
  }

  @Override
  public void open() throws IOException
  {
    flattener.open();
  }

  @Override
  protected void addValue(int val) throws IOException
  {
    if (!endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer = ByteBuffer.allocate(chunkBytes).order(byteOrder);
      endBuffer.limit(numBytes * chunkFactor);
    }
    intBuffer.putInt(0, val);
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
      endBuffer.put(intBuffer.array(), Ints.BYTES - numBytes, numBytes);
    } else {
      endBuffer.put(intBuffer.array(), 0, numBytes);
    }
    numInserted++;
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
    meta.put(VERSION);
    meta.put((byte) numBytes);
    meta.putInt(numInserted);
    meta.putInt(chunkFactor);
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
    return 1 +             // version
           1 +             // numBytes
           Ints.BYTES +    // numInserted
           Ints.BYTES +    // chunkFactor
           1;              // compression id
  }
}
