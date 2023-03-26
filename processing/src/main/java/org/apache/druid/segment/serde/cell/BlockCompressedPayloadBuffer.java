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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BlockCompressedPayloadBuffer implements Closeable
{
  private final ByteBuffer currentBlock;
  private final ByteBuffer compressedByteBuffer;
  private final BlockIndexWriter blockIndexWriter;
  private final WriteOutBytes dataOutBytes;
  private final Closer closer;
  private final CompressionStrategy.Compressor compressor;

  private boolean open = true;

  public BlockCompressedPayloadBuffer(
      ByteBuffer currentBlock,
      ByteBuffer compressedByteBuffer,
      BlockIndexWriter blockIndexWriter,
      WriteOutBytes dataOutBytes,
      Closer closer,
      CompressionStrategy.Compressor compressor
  )
  {
    currentBlock.clear();
    compressedByteBuffer.clear();
    this.currentBlock = currentBlock;
    this.compressedByteBuffer = compressedByteBuffer;
    this.closer = closer;
    this.blockIndexWriter = blockIndexWriter;
    this.dataOutBytes = dataOutBytes;
    this.compressor = compressor;
  }

  public void write(byte[] payload) throws IOException
  {
    Preconditions.checkNotNull(payload);
    write(ByteBuffer.wrap(payload).order(ByteOrder.nativeOrder()));
  }

  public void write(ByteBuffer masterPayload) throws IOException
  {
    Preconditions.checkNotNull(masterPayload);
    Preconditions.checkState(open, "cannot write to closed BlockCompressedPayloadWriter");
    ByteBuffer payload = masterPayload.asReadOnlyBuffer().order(masterPayload.order());

    while (payload.hasRemaining()) {
      int writeSize = Math.min(payload.remaining(), currentBlock.remaining());

      payload.limit(payload.position() + writeSize);
      currentBlock.put(payload);

      if (!currentBlock.hasRemaining()) {
        flush();
      }

      payload.limit(masterPayload.limit());
    }
  }

  @Override
  public void close() throws IOException
  {
    closer.close();
  }

  public BlockCompressedPayloadSerializer closeToSerializer() throws IOException
  {
    if (open) {
      if (currentBlock.position() > 0) {
        flush();
      }

      blockIndexWriter.close();
      closer.close();
      open = false;
    }

    return new BlockCompressedPayloadSerializer(blockIndexWriter, dataOutBytes);
  }

  private void flush() throws IOException
  {
    Preconditions.checkState(open, "flush() on closed BlockCompressedPayloadWriter");
    currentBlock.flip();

    ByteBuffer actualCompressedByteBuffer = compressor.compress(currentBlock, compressedByteBuffer);
    int compressedBlockSize = actualCompressedByteBuffer.limit();

    blockIndexWriter.persistAndIncrement(compressedBlockSize);
    dataOutBytes.write(actualCompressedByteBuffer);
    currentBlock.clear();
    compressedByteBuffer.clear();
  }
}
