/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.guava.CloseQuietly;
import com.ning.compress.lzf.ChunkEncoder;
import com.ning.compress.lzf.LZFChunk;
import com.ning.compress.lzf.LZFDecoder;
import io.druid.collections.ResourceHolder;
import io.druid.segment.CompressedPools;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 */
public class CompressedObjectStrategy<T extends Buffer> implements ObjectStrategy<ResourceHolder<T>>
{
  public static final CompressionStrategy DEFAULT_COMPRESSION_STRATEGY = CompressionStrategy.LZ4;

  public static enum CompressionStrategy
  {
    LZF((byte) 0x0)
        {
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

    LZ4((byte) 0x1)
        {
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
    UNCOMPRESSED((byte) 0x2)
        {
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
        Throwables.propagate(e);
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
      try(final ResourceHolder<ChunkEncoder> encoder = CompressedPools.getChunkEncoder()) {
        LZFChunk chunk = encoder.get().encodeChunk(bytes, 0, bytes.length);
        return chunk.getData();
      }
      catch (IOException e) {
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
      final byte[] bytes = new byte[numBytes];
      in.get(bytes);

      try (final ResourceHolder<byte[]> outputBytesHolder = CompressedPools.getOutputBytes()) {
        final byte[] outputBytes = outputBytesHolder.get();
        // Since decompressed size is NOT known, must use lz4Safe
        final int numDecompressedBytes = lz4Safe.decompress(bytes,outputBytes);
        out.put(outputBytes, 0, numDecompressedBytes);
        out.flip();
      }
      catch (IOException e) {
        Throwables.propagate(e);
      }
    }

    @Override
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out, int decompressedSize)
    {
      final byte[] bytes = new byte[numBytes];
      in.get(bytes);

      // TODO: Upgrade this to ByteBuffer once https://github.com/jpountz/lz4-java/issues/9 is in mainline code for lz4-java
      try (final ResourceHolder<byte[]> outputBytesHolder = CompressedPools.getOutputBytes()) {
        final byte[] outputBytes = outputBytesHolder.get();
        lz4Fast.decompress(bytes, 0, outputBytes, 0, decompressedSize);

        out.put(outputBytes, 0, decompressedSize);
        out.flip();
      }
      catch (IOException e) {
        Throwables.propagate(e);
      }
    }
  }

  public static class LZ4Compressor implements Compressor
  {
    private static final LZ4Compressor defaultCompressor = new LZ4Compressor();
    private static final net.jpountz.lz4.LZ4Compressor lz4Fast = LZ4Factory.fastestInstance().fastCompressor();
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
