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

import io.druid.common.utils.ByteUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.IndexIO;
import io.druid.segment.serde.MetaSerdeHelper;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Streams array of integers out in the binary format described by {@link CompressedVSizeColumnarIntsSupplier}
 */
public class CompressedVSizeColumnarIntsSerializer extends SingleValueColumnarIntsSerializer
{
  private static final byte VERSION = CompressedVSizeColumnarIntsSupplier.VERSION;

  private static final MetaSerdeHelper<CompressedVSizeColumnarIntsSerializer> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((CompressedVSizeColumnarIntsSerializer x) -> VERSION)
      .writeByte(x -> ByteUtils.checkedCast(x.numBytes))
      .writeInt(x -> x.numInserted)
      .writeInt(x -> x.chunkFactor)
      .writeByte(x -> x.compression.getId());

  public static CompressedVSizeColumnarIntsSerializer create(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final String filenameBase,
      final int maxValue,
      final CompressionStrategy compression
  )
  {
    return new CompressedVSizeColumnarIntsSerializer(
        segmentWriteOutMedium,
        filenameBase,
        maxValue,
        CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue),
        IndexIO.BYTE_ORDER,
        compression
    );
  }

  private final int numBytes;
  private final int chunkFactor;
  private final boolean isBigEndian;
  private final CompressionStrategy compression;
  private final GenericIndexedWriter<ByteBuffer> flattener;
  private final ByteBuffer intBuffer;

  private ByteBuffer endBuffer;
  private int numInserted;

  CompressedVSizeColumnarIntsSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final String filenameBase,
      final int maxValue,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressionStrategy compression
  )
  {
    this(
        segmentWriteOutMedium,
        maxValue,
        chunkFactor,
        byteOrder,
        compression,
        GenericIndexedWriter.ofCompressedByteBuffers(
            segmentWriteOutMedium,
            filenameBase,
            compression,
            sizePer(maxValue, chunkFactor)
        )
    );
  }

  CompressedVSizeColumnarIntsSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final int maxValue,
      final int chunkFactor,
      final ByteOrder byteOrder,
      final CompressionStrategy compression,
      final GenericIndexedWriter<ByteBuffer> flattener
  )
  {
    this.numBytes = VSizeColumnarInts.getNumBytesForMax(maxValue);
    this.chunkFactor = chunkFactor;
    int chunkBytes = chunkFactor * numBytes;
    this.isBigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
    this.compression = compression;
    this.flattener = flattener;
    this.intBuffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
    CompressionStrategy.Compressor compressor = compression.getCompressor();
    this.endBuffer = compressor.allocateInBuffer(chunkBytes, segmentWriteOutMedium.getCloser()).order(byteOrder);
    this.numInserted = 0;
  }

  private static int sizePer(int maxValue, int chunkFactor)
  {
    return chunkFactor * VSizeColumnarInts.getNumBytesForMax(maxValue)
           + CompressedVSizeColumnarIntsSupplier.bufferPadding(VSizeColumnarInts.getNumBytesForMax(maxValue));
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
    intBuffer.putInt(0, val);
    if (isBigEndian) {
      endBuffer.put(intBuffer.array(), Integer.BYTES - numBytes, numBytes);
    } else {
      endBuffer.put(intBuffer.array(), 0, numBytes);
    }
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
