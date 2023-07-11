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

import org.apache.druid.segment.data.CompressionStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class BlockCompressedPayloadWriterToBytesWriter implements BytesWriter
{
  private final BlockCompressedPayloadWriter blockCompressedPayloadWriter;

  public BlockCompressedPayloadWriterToBytesWriter(BlockCompressedPayloadWriter blockCompressedPayloadWriter)
  {
    this.blockCompressedPayloadWriter = blockCompressedPayloadWriter;
  }

  @Override
  public void write(byte[] payload) throws IOException
  {
    blockCompressedPayloadWriter.write(payload);
  }

  @Override
  public void write(ByteBuffer cellByteBuffer) throws IOException
  {
    blockCompressedPayloadWriter.write(cellByteBuffer);
  }

  @Override
  public void close() throws IOException
  {
    blockCompressedPayloadWriter.close();
  }

  @Override
  public void transferTo(WritableByteChannel channel) throws IOException
  {
    blockCompressedPayloadWriter.writeTo(channel, null);
  }

  @Override
  public long getSerializedSize()
  {
    return blockCompressedPayloadWriter.getSerializedSize();
  }

  public static class Builder implements BytesWriterBuilder
  {
    private final BlockCompressedPayloadWriter.Builder builder;

    public Builder(BlockCompressedPayloadWriter.Builder builder)
    {
      this.builder = builder;
    }

    @Override
    public BytesWriter build() throws IOException
    {
      return new BlockCompressedPayloadWriterToBytesWriter(builder.build());
    }

    @Override
    public BytesWriterBuilder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      builder.setCompressionStrategy(compressionStrategy);

      return this;
    }

    @Override
    public BytesWriterBuilder setByteBufferProvider(ByteBufferProvider byteBufferProvider)
    {
      builder.setByteBufferProvider(byteBufferProvider);

      return this;
    }
  }
}
