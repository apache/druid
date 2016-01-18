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

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.ning.compress.BufferRecycler;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import io.druid.collections.ResourceHolder;
import io.druid.segment.CompressedPools;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 */
public class CompressedObjectStrategy<T extends Buffer> implements ObjectStrategy<ResourceHolder<T>>
{
  private static final Logger log = new Logger(CompressedObjectStrategy.class);
  public static final CompressionStrategy DEFAULT_COMPRESSION_STRATEGY = CompressionStrategy.LZ4;

  public static enum CompressionStrategy
  {
    LZF((byte) 0x0) {
      @Override
      public Decompressor getDecompressor()
      {
        return LZFDecompressor.defaultDecompressor;
      }

      @Override
      public Compressor getCompressor()
      {
        return LZFCompressor.defaultCompressor;
      }
    },

    LZ4((byte) 0x1) {
      @Override
      public Decompressor getDecompressor()
      {
        return LZ4Decompressor.defaultDecompressor;
      }

      @Override
      public Compressor getCompressor()
      {
        return LZ4Compressor.defaultCompressor;
      }
    },
    UNCOMPRESSED((byte) 0xFF) {
      @Override
      public Decompressor getDecompressor()
      {
        return UncompressedDecompressor.defaultDecompressor;
      }

      @Override
      public Compressor getCompressor()
      {
        return UncompressedCompressor.defaultCompressor;
      }
    };

    final byte id;

    CompressionStrategy(byte id)
    {
      this.id = id;
    }

    public byte getId()
    {
      return id;
    }

    public abstract Compressor getCompressor();

    public abstract Decompressor getDecompressor();

    static final Map<Byte, CompressionStrategy> idMap = Maps.newHashMap();

    static {
      for (CompressionStrategy strategy : CompressionStrategy.values()) {
        idMap.put(strategy.getId(), strategy);
      }
    }

    public static CompressionStrategy forId(byte id)
    {
      return idMap.get(id);
    }
  }

  public static interface Decompressor
  {
    /**
     * Implementations of this method are expected to call out.flip() after writing to the output buffer
     *
     * @param in
     * @param numBytes
     * @param out
     */
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out);

    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out, int decompressedSize);
  }

  public static interface Compressor
  {
    /**
     * Currently assumes buf is an array backed ByteBuffer
     *
     * @param bytes
     *
     * @return
     */
    public byte[] compress(byte[] bytes);
  }

  public static class UncompressedCompressor implements Compressor
  {
    private static final UncompressedCompressor defaultCompressor = new UncompressedCompressor();

    @Override
    public byte[] compress(byte[] bytes)
    {
      return bytes;
    }
  }

  public static class UncompressedDecompressor implements Decompressor
  {
    private static final UncompressedDecompressor defaultDecompressor = new UncompressedDecompressor();

    @Override
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out)
    {
      final ByteBuffer copyBuffer = in.duplicate();
      copyBuffer.limit(copyBuffer.position() + numBytes);
      out.put(copyBuffer).flip();
      in.position(in.position() + numBytes);
    }

    @Override
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out, int decompressedSize)
    {
      decompress(in, numBytes, out);
    }
  }

  public static class LZFDecompressor implements Decompressor
  {
    private static final LZFDecompressor defaultDecompressor = new LZFDecompressor();

    @Override
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out)
    {
      final byte[] bytes = new byte[numBytes];
      in.get(bytes);

      try (final ResourceHolder<byte[]> outputBytesHolder = CompressedPools.getOutputBytes()) {
        final byte[] outputBytes = outputBytesHolder.get();
        final int numDecompressedBytes = LZFDecoder.decode(bytes, outputBytes);
        out.put(outputBytes, 0, numDecompressedBytes);
        out.flip();
      }
      catch (IOException e) {
        log.error(e, "Error decompressing data");
      }
    }

    @Override
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out, int decompressedSize)
    {
      decompress(in, numBytes, out);
    }
  }

  public static class LZFCompressor implements Compressor
  {
    private static final LZFCompressor defaultCompressor = new LZFCompressor();

    @Override
    public byte[] compress(byte[] bytes)
    {

      try (final ResourceHolder<BufferRecycler> bufferRecycler = CompressedPools.getBufferRecycler()) {
        return LZFEncoder.encode(bytes, 0, bytes.length, bufferRecycler.get());
      }
      catch (IOException e) {
        log.error(e, "Error compressing data");
        throw Throwables.propagate(e);
      }
    }
  }

  public static class LZ4Decompressor implements Decompressor
  {
    private static final LZ4SafeDecompressor lz4Safe = LZ4Factory.fastestInstance().safeDecompressor();
    private static final LZ4FastDecompressor lz4Fast = LZ4Factory.fastestInstance().fastDecompressor();
    private static final LZ4Decompressor defaultDecompressor = new LZ4Decompressor();

    @Override
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out)
    {
      // Since decompressed size is NOT known, must use lz4Safe
      // lz4Safe.decompress does not modify buffer positions
      final int numDecompressedBytes = lz4Safe.decompress(in, in.position(), numBytes, out, out.position(), out.remaining());
      out.limit(out.position() + numDecompressedBytes);
    }

    @Override
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out, int decompressedSize)
    {
      // lz4Fast.decompress does not modify buffer positions
      lz4Fast.decompress(in, in.position(), out, out.position(), decompressedSize);
      out.limit(out.position() + decompressedSize);
    }
  }

  public static class LZ4Compressor implements Compressor
  {
    private static final LZ4Compressor defaultCompressor = new LZ4Compressor();
    private static final net.jpountz.lz4.LZ4Compressor lz4High = LZ4Factory.fastestInstance().highCompressor();

    @Override
    public byte[] compress(byte[] bytes)
    {
      return lz4High.compress(bytes);
    }
  }

  protected final ByteOrder order;
  protected final BufferConverter<T> converter;
  protected final Decompressor decompressor;
  private final Compressor compressor;

  protected CompressedObjectStrategy(
      final ByteOrder order,
      final BufferConverter<T> converter,
      final CompressionStrategy compression
  )
  {
    this.order = order;
    this.converter = converter;
    this.decompressor = compression.getDecompressor();
    this.compressor = compression.getCompressor();
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
    final ResourceHolder<ByteBuffer> bufHolder = CompressedPools.getByteBuf(order);
    final ByteBuffer buf = bufHolder.get();
    buf.position(0);
    buf.limit(buf.capacity());

    decompress(buffer, numBytes, buf);
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

  protected void decompress(
      ByteBuffer buffer,
      int numBytes,
      ByteBuffer buf
  )
  {
    decompressor.decompress(buffer, numBytes, buf);
  }

  @Override
  public byte[] toBytes(ResourceHolder<T> holder)
  {
    T val = holder.get();
    ByteBuffer buf = bufferFor(val);
    converter.combine(buf, val);
    return compressor.compress(buf.array());
  }

  protected ByteBuffer bufferFor(T val)
  {
    return ByteBuffer.allocate(converter.sizeOf(val.remaining())).order(order);
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
