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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BlockCompressedPayloadBufferFactory
{
  private final ByteBufferProvider byteBufferProvider;
  private final SegmentWriteOutMedium writeOutMedium;
  private final CompressionStrategy.Compressor compressor;

  public BlockCompressedPayloadBufferFactory(
      ByteBufferProvider byteBufferProvider,
      SegmentWriteOutMedium writeOutMedium,
      CompressionStrategy.Compressor compressor
  )
  {
    this.byteBufferProvider = byteBufferProvider;
    this.writeOutMedium = writeOutMedium;
    this.compressor = compressor;
  }

  public BlockCompressedPayloadBuffer create() throws IOException
  {
    Closer closer = Closer.create();
    ResourceHolder<ByteBuffer> currentBlockHolder = byteBufferProvider.get();

    closer.register(currentBlockHolder);

    ByteBuffer compressedBlockByteBuffer = compressor.allocateOutBuffer(currentBlockHolder.get().limit(), closer);

    return new BlockCompressedPayloadBuffer(
        currentBlockHolder.get(),
        compressedBlockByteBuffer,
        new BlockIndexWriter(writeOutMedium.makeWriteOutBytes()),
        writeOutMedium.makeWriteOutBytes(),
        closer,
        compressor
    );
  }
}
