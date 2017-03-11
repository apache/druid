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

import io.druid.collections.ResourceHolder;
import io.druid.segment.CompressedPools;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DecompressingByteBufferObjectStrategy extends ObjectStrategy<ResourceHolder<ByteBuffer>>
{
  private final ByteOrder order;
  private final CompressionStrategy.Decompressor decompressor;
  private final int sizePerInBytes;

  DecompressingByteBufferObjectStrategy(ByteOrder order, CompressionStrategy compression, int sizePerInBytes)
  {
    this.order = order;
    this.decompressor = compression.getDecompressor();
    this.sizePerInBytes = sizePerInBytes;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<? extends ResourceHolder<ByteBuffer>> getClazz()
  {
    return (Class) ResourceHolder.class;
  }

  private void decompress(ByteBuffer buffer, int numBytes, ByteBuffer buf)
  {
    decompressor.decompress(buffer, numBytes, buf, sizePerInBytes);
  }

  @Override
  public ResourceHolder<ByteBuffer> fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    final ResourceHolder<ByteBuffer> bufHolder = CompressedPools.getByteBuf(order);
    final ByteBuffer buf = bufHolder.get();
    buf.clear();

    decompress(buffer, numBytes, buf);
    return new ResourceHolder<ByteBuffer>()
    {
      @Override
      public ByteBuffer get()
      {
        return buf;
      }

      @Override
      public void close()
      {
        bufHolder.close();
      }
    };
  }

  @Override
  public int compare(ResourceHolder<ByteBuffer> o1, ResourceHolder<ByteBuffer> o2)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] toBytes(ResourceHolder<ByteBuffer> holder)
  {
    throw new UnsupportedOperationException();
  }
}
