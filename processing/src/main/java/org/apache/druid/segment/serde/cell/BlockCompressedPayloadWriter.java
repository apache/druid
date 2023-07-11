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
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class BlockCompressedPayloadWriter implements Serializer, Closeable
{
  private final BlockCompressedPayloadBuffer buffer;
  private BlockCompressedPayloadSerializer serializer;
  private State state = State.OPEN;

  private BlockCompressedPayloadWriter(BlockCompressedPayloadBuffer buffer)
  {
    this.buffer = buffer;
  }

  public void write(byte[] payload) throws IOException
  {
    Preconditions.checkState(state == State.OPEN);
    buffer.write(payload);
  }

  public void write(ByteBuffer payload) throws IOException
  {
    Preconditions.checkState(state == State.OPEN);
    buffer.write(payload);
  }

  @Override
  public void close() throws IOException
  {
    if (state == State.OPEN) {
      serializer = buffer.closeToSerializer();
      state = State.CLOSED;
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel, @Nullable FileSmoosher smoosher) throws IOException
  {
    Preconditions.checkState(state == State.CLOSED);
    serializer.writeTo(channel, smoosher);
  }

  @Override
  public long getSerializedSize()
  {
    Preconditions.checkState(state == State.CLOSED);
    return serializer.getSerializedSize();
  }

  private enum State
  {
    OPEN,
    CLOSED
  }

  public static class Builder
  {
    private ByteBufferProvider byteBufferProvider = NativeClearedByteBufferProvider.INSTANCE;
    private final SegmentWriteOutMedium writeOutMedium;

    private CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;

    public Builder(SegmentWriteOutMedium writeOutMedium)
    {
      this.writeOutMedium = writeOutMedium;
    }

    public Builder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      this.compressionStrategy = compressionStrategy;

      return this;
    }

    public Builder setByteBufferProvider(ByteBufferProvider byteBufferProvider)
    {
      this.byteBufferProvider = byteBufferProvider;

      return this;
    }

    public BlockCompressedPayloadWriter build() throws IOException
    {
      BlockCompressedPayloadBufferFactory bufferFactory = new BlockCompressedPayloadBufferFactory(
          byteBufferProvider,
          writeOutMedium,
          compressionStrategy.getCompressor()
      );
      BlockCompressedPayloadBuffer payloadBuffer = bufferFactory.create();
      BlockCompressedPayloadWriter payloadWriter = new BlockCompressedPayloadWriter(payloadBuffer);

      return payloadWriter;
    }
  }
}
