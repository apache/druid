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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Maps;
import com.ning.compress.BufferRecycler;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.ByteBufferUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.Closer;
import io.druid.segment.CompressedPools;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Compression strategy is used to compress block of bytes without knowledge of what data the bytes represents.
 *
 * When adding compression strategy, do not use id in the range [0x7C, 0xFD] (greater than 123 or less than -2), since
 * a flag mechanism is used in CompressionFactory that involves subtracting the value 126 from the compression id
 * (see {@link CompressionFactory#FLAG_BOUND})
 */
public enum CompressionStrategy
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
  },
  /*
  This value indicate no compression strategy should be used, and compression should not be block based
  Currently only IndexedLong support non block based compression, and other types treat this as UNCOMPRESSED
   */
  NONE((byte) 0xFE) {
    @Override
    public Decompressor getDecompressor()
    {
      throw new UnsupportedOperationException("NONE compression strategy shouldn't use any decompressor");
    }

    @Override
    public Compressor getCompressor()
    {
      throw new UnsupportedOperationException("NONE compression strategy shouldn't use any compressor");
    }
  };
  public static final CompressionStrategy DEFAULT_COMPRESSION_STRATEGY = LZ4;

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

  @JsonValue
  @Override
  public String toString()
  {
    return StringUtils.toLowerCase(this.name());
  }

  @JsonCreator
  public static CompressionStrategy fromString(String name)
  {
    return valueOf(StringUtils.toUpperCase(name));
  }

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

  // TODO remove this method and change all its callers to use all CompressionStrategy values when NONE type is supported by all types
  public static CompressionStrategy[] noNoneValues()
  {
    return (CompressionStrategy[]) ArrayUtils.removeElement(CompressionStrategy.values(), NONE);
  }

  public interface Decompressor
  {
    /**
     * Implementations of this method are expected to call out.flip() after writing to the output buffer
     */
    void decompress(ByteBuffer in, int numBytes, ByteBuffer out);
  }

  public static abstract class Compressor
  {
    /**
     * Allocates a buffer that should be passed to {@link #compress} method as input buffer. Different Compressors
     * require (or work more efficiently with) different kinds of buffers.
     *
     * If the allocated buffer is a direct buffer, it should be registered to be freed with the given Closer.
     */
    ByteBuffer allocateInBuffer(int inputSize, Closer closer)
    {
      return ByteBuffer.allocate(inputSize);
    }

    /**
     * Allocates a buffer that should be passed to {@link #compress} method as output buffer. Different Compressors
     * require (or work more efficiently with) different kinds of buffers.
     *
     * Allocates a buffer that is always enough to compress a byte sequence of the given size.
     *
     * If the allocated buffer is a direct buffer, it should be registered to be freed with the given Closer.
     */
    abstract ByteBuffer allocateOutBuffer(int inputSize, Closer closer);

    /**
     * Returns a ByteBuffer with compressed contents of in between it's position and limit. It may be the provided out
     * ByteBuffer, or the in ByteBuffer, depending on the implementation. {@code out}'s position and limit
     * are not respected and could be discarded.
     *
     * <p>Contents of {@code in} between it's position and limit are compressed. It's contents, position and limit
     * shouldn't be changed in compress() method.
     */
    public abstract ByteBuffer compress(ByteBuffer in, ByteBuffer out);
  }

  public static class UncompressedCompressor extends Compressor
  {
    private static final UncompressedCompressor defaultCompressor = new UncompressedCompressor();

    @Override
    ByteBuffer allocateOutBuffer(int inputSize, Closer closer)
    {
      return ByteBuffer.allocate(inputSize);
    }

    @Override
    public ByteBuffer compress(ByteBuffer in, ByteBuffer out)
    {
      return in;
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
        throw new RuntimeException(e);
      }
    }

  }

  public static class LZFCompressor extends Compressor
  {
    private static final LZFCompressor defaultCompressor = new LZFCompressor();

    @Override
    public ByteBuffer allocateOutBuffer(int inputSize, Closer closer)
    {
      return ByteBuffer.allocate(LZFEncoder.estimateMaxWorkspaceSize(inputSize));
    }

    @Override
    public ByteBuffer compress(ByteBuffer in, ByteBuffer out)
    {
      try (final ResourceHolder<BufferRecycler> bufferRecycler = CompressedPools.getBufferRecycler()) {
        int encodedLen = LZFEncoder.appendEncoded(
            in.array(),
            in.arrayOffset() + in.position(),
            in.remaining(),
            out.array(),
            out.arrayOffset(),
            bufferRecycler.get()
        );
        out.clear();
        out.limit(encodedLen);
        return out;
      }
    }
  }

  public static class LZ4Decompressor implements Decompressor
  {
    private static final LZ4SafeDecompressor lz4Safe = LZ4Factory.fastestInstance().safeDecompressor();
    private static final LZ4Decompressor defaultDecompressor = new LZ4Decompressor();

    @Override
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out)
    {
      // Since decompressed size is NOT known, must use lz4Safe
      // lz4Safe.decompress does not modify buffer positions
      final int numDecompressedBytes = lz4Safe.decompress(
          in,
          in.position(),
          numBytes,
          out,
          out.position(),
          out.remaining()
      );
      out.limit(out.position() + numDecompressedBytes);
    }

  }

  public static class LZ4Compressor extends Compressor
  {
    private static final LZ4Compressor defaultCompressor = new LZ4Compressor();
    private static final net.jpountz.lz4.LZ4Compressor lz4High = LZ4Factory.fastestInstance().highCompressor();

    @Override
    ByteBuffer allocateInBuffer(int inputSize, Closer closer)
    {
      ByteBuffer inBuffer = ByteBuffer.allocateDirect(inputSize);
      closer.register(() -> ByteBufferUtils.free(inBuffer));
      return inBuffer;
    }

    @Override
    ByteBuffer allocateOutBuffer(int inputSize, Closer closer)
    {
      ByteBuffer outBuffer = ByteBuffer.allocateDirect(lz4High.maxCompressedLength(inputSize));
      closer.register(() -> ByteBufferUtils.free(outBuffer));
      return outBuffer;
    }

    @Override
    public ByteBuffer compress(ByteBuffer in, ByteBuffer out)
    {
      out.clear();
      int position = in.position();
      lz4High.compress(in, out);
      in.position(position);
      out.flip();
      return out;
    }
  }
}
