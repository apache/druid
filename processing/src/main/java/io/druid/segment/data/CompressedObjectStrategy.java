/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.data;

import com.google.common.base.Throwables;
import com.metamx.common.guava.CloseQuietly;
import com.ning.compress.lzf.ChunkEncoder;
import com.ning.compress.lzf.LZFChunk;
import com.ning.compress.lzf.LZFDecoder;
import io.druid.collections.ResourceHolder;
import io.druid.segment.CompressedPools;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
*/
public class CompressedObjectStrategy<T extends Buffer> implements ObjectStrategy<ResourceHolder<T>>
{
  private final ByteOrder order;
  private final BufferConverter<T> converter;

  protected CompressedObjectStrategy(
      final ByteOrder order,
      final BufferConverter<T> converter
  )
  {
    this.order = order;
    this.converter = converter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<? extends ResourceHolder<T>> getClazz()
  {
    return (Class) ResourceHolder.class;
  }

  @Override
  public ResourceHolder<T> fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    byte[] bytes = new byte[numBytes];
    buffer.get(bytes);

    final ResourceHolder<ByteBuffer> bufHolder = CompressedPools.getByteBuf(order);
    final ByteBuffer buf = bufHolder.get();
    buf.position(0);
    buf.limit(buf.capacity());

    try {
      final ResourceHolder<byte[]> outputBytesHolder = CompressedPools.getOutputBytes();

      byte[] outputBytes = outputBytesHolder.get();
      int numDecompressedBytes = LZFDecoder.decode(bytes, outputBytes);
      buf.put(outputBytes, 0, numDecompressedBytes);
      buf.flip();

      CloseQuietly.close(outputBytesHolder);

      return new ResourceHolder<T>()
      {
        @Override
        public T get()
        {
          return converter.convert(buf);
        }

        @Override
        public void close() throws IOException
        {
          bufHolder.close();
        }
      };
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public byte[] toBytes(ResourceHolder<T> holder)
  {
    T val = holder.get();
    ByteBuffer buf = ByteBuffer.allocate(converter.sizeOf(val.remaining())).order(order);
    converter.combine(buf, val);

    final ResourceHolder<ChunkEncoder> encoder = CompressedPools.getChunkEncoder();
    LZFChunk chunk = encoder.get().encodeChunk(buf.array(), 0, buf.array().length);
    CloseQuietly.close(encoder);

    return chunk.getData();
  }

  @Override
  public int compare(ResourceHolder<T> o1, ResourceHolder<T> o2)
  {
    return converter.compare(o1.get(), o2.get());
  }

  public static interface BufferConverter<T>
  {
    public T convert(ByteBuffer buf);
    public int compare(T lhs, T rhs);
    public int sizeOf(int count);
    public T combine(ByteBuffer into, T from);
  }
}
