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

package org.apache.druid.segment.serde.cell;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.data.CompressionStrategy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 *  CellReader is intended to read the data written byte {@link CellWriter}. The {@code CellWriter.writeTo()} method's
 *  output must be made available as a ByteBuffer. While this provides relatively efficient random access, it is
 *  optimized for sequential access by caching the last decompressed block in both the index (which is
 *  block-compressed) and the data.
 *  <p/>
 *  A random access incurs the following costs:
 *  <pre>
 *    1. seek to compressed block location in index
 *    2. decompress index block
 *    3. read data location
 *    4. decompress data block
 *    5. wrap or copy data from uncompressed block (copy for data that spans more than one block)<br>
 *  </pre>
 *  Sequential access amortizes the decompression cost by storing the last decompressed block (cache of size 1,
 *  effectively).
 *  <p/>
 *  Note also that the index itself is compressed, so random accesses potentially incur an additional decompression
 *  step for large datasets.
 *  <p/>
 *  <pre>{@code
 *  ByteBuffer byteBuffer = ....; // ByteBuffer created from writableChannel output of CellWriter.writeTo()
 *  try (CellRead cellReader = new CellReader.Builder(byteBuffer).build()) {
 *    for (int i = 0; i < numPayloads; i++) {
 *      byte[] payload = cellReader.getCell(i);
 *
 *      processPayload(payload); // may deserialize and peform work
 *    }
 *  }
 *  </pre>
 *
 *  While you may allocate your own 64k buffers, it is recommended you use {@code NativeClearedByteBufferProvider}
 *  which provides direct 64k ByteBuffers from a pool, wrapped in a ResourceHolder. These objects may be
 *  registered in a Closer
 *
 *  To enhance future random accesss, a decompressed block cache may be added of some size k (=10, etc)
 *  At present, we effecitively have a block cache of size 1
 */
public class CellReader implements Closeable
{
  private final CellIndexReader cellIndexReader;
  private final BlockCompressedPayloadReader dataReader;
  private final Closer closer;

  private CellReader(CellIndexReader cellIndexReader, BlockCompressedPayloadReader dataReader, Closer closer)
  {
    this.cellIndexReader = cellIndexReader;
    this.dataReader = dataReader;
    this.closer = closer;
  }

  public ByteBuffer getCell(int rowNumber)
  {
    PayloadEntrySpan payloadEntrySpan = cellIndexReader.getEntrySpan(rowNumber);
    ByteBuffer payload = dataReader.read(payloadEntrySpan.getStart(), payloadEntrySpan.getSize());

    return payload;
  }

  @Override
  public void close() throws IOException
  {
    closer.close();
  }

  public static Builder builder(ByteBuffer originalByteBuffer)
  {
    return new Builder(originalByteBuffer);
  }

  public static class Builder
  {
    private final ByteBuffer cellIndexBuffer;
    private final ByteBuffer dataStorageBuffer;

    private CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;
    private ByteBufferProvider byteBufferProvider = NativeClearedByteBufferProvider.INSTANCE;

    /**
     * The default block size is 64k as provided by NativeClearedByteBufferProvider. You may change this, but
     * be sure the size of the ByteBuffers match what the CelLWriter used as this is the block size stored. All
     * reading will fail unpredictably if a different block size is used when reading
     *
     * @param originalByteBuffer - buffer from {@code CellWriter.writeTo()} as written to the WritableChannel
     */
    public Builder(ByteBuffer originalByteBuffer)
    {
      ByteBuffer masterByteBuffer = originalByteBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());

      int cellIndexSize = masterByteBuffer.getInt();
      cellIndexBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
      cellIndexBuffer.limit(cellIndexBuffer.position() + cellIndexSize);

      masterByteBuffer.position(masterByteBuffer.position() + cellIndexSize);

      int dataStorageSize = masterByteBuffer.getInt();
      dataStorageBuffer = masterByteBuffer.asReadOnlyBuffer().order(masterByteBuffer.order());
      dataStorageBuffer.limit(dataStorageBuffer.position() + dataStorageSize);
    }

    /**
     *
     * @param compressionStrategy - this must match the CellWriter compressionStrategy. Default LZ4
     * @return
     */
    public Builder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      this.compressionStrategy = compressionStrategy;

      return this;
    }

    /**
     *
     * @param byteBufferProvider - ByteBuffers returned must match the size used in the CellWriter
     * @return Builder
     */
    public Builder setByteBufferProvider(ByteBufferProvider byteBufferProvider)
    {
      this.byteBufferProvider = byteBufferProvider;

      return this;
    }

    public CellReader build()
    {
      Closer closer = Closer.create();
      CellIndexReader cellIndexReader = new CellIndexReader(BlockCompressedPayloadReader.create(
          cellIndexBuffer,
          byteBufferProvider,
          compressionStrategy.getDecompressor()
      ));
      BlockCompressedPayloadReader dataReader = BlockCompressedPayloadReader.create(
          dataStorageBuffer,
          byteBufferProvider,
          compressionStrategy.getDecompressor()
      );

      closer.register(cellIndexReader);
      closer.register(dataReader);

      CellReader cellReader = new CellReader(cellIndexReader, dataReader, closer);

      return cellReader;
    }
  }
}
