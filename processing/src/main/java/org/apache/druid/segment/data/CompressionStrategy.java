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

package org.apache.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.ning.compress.BufferRecycler;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.commons.lang.ArrayUtils;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.CompressedPools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
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
      return LZFDecompressor.DEFAULT_DECOMPRESSOR;
    }

    @Override
    public Compressor getCompressor()
    {
      return LZFCompressor.DEFAULT_COMPRESSOR;
    }
  },

  LZ4((byte) 0x1) {
    @Override
    public Decompressor getDecompressor()
    {
      return LZ4Decompressor.DEFAULT_COMPRESSOR;
    }

    @Override
    public Compressor getCompressor()
    {
      return LZ4Compressor.DEFAULT_COMPRESSOR;
    }
  },
  UNCOMPRESSED((byte) 0xFF) {
    @Override
    public Decompressor getDecompressor()
    {
      return UncompressedDecompressor.DEFAULT_DECOMPRESSOR;
    }

    @Override
    public Compressor getCompressor()
    {
      return UncompressedCompressor.DEFAULT_COMPRESSOR;
    }
  },
  /**
   * This value indicate no compression strategy should be used, and compression should not be block based.
   * {@link ColumnarLongs}, {@link ColumnarFloats} and {@link ColumnarDoubles} support non block based compression, and
   * other types treat this as {@link #UNCOMPRESSED}.
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
  private static final Logger LOG = new Logger(CompressionStrategy.class);

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

  static final Map<Byte, CompressionStrategy> ID_MAP = new HashMap<>();

  static {
    for (CompressionStrategy strategy : CompressionStrategy.values()) {
      ID_MAP.put(strategy.getId(), strategy);
    }
  }

  public static CompressionStrategy forId(byte id)
  {
    return ID_MAP.get(id);
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

  public abstract static class Compressor
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
    private static final UncompressedCompressor DEFAULT_COMPRESSOR = new UncompressedCompressor();

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
    private static final UncompressedDecompressor DEFAULT_DECOMPRESSOR = new UncompressedDecompressor();

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
    private static final LZFDecompressor DEFAULT_DECOMPRESSOR = new LZFDecompressor();

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
    private static final LZFCompressor DEFAULT_COMPRESSOR = new LZFCompressor();

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
    private static final LZ4SafeDecompressor LZ4_SAFE = LZ4Factory.fastestInstance().safeDecompressor();
    private static final LZ4Decompressor DEFAULT_COMPRESSOR = new LZ4Decompressor();

    @Override
    public void decompress(ByteBuffer in, int numBytes, ByteBuffer out)
    {
      // Since decompressed size is NOT known, must use lz4Safe
      // lz4Safe.decompress does not modify buffer positions
      final int numDecompressedBytes = LZ4_SAFE.decompress(
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
    private static final LZ4Compressor DEFAULT_COMPRESSOR = new LZ4Compressor();
    private static final net.jpountz.lz4.LZ4Compressor LZ4_HIGH = LZ4Factory.fastestInstance().highCompressor();

    static {
      logLZ4State();
    }

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
      ByteBuffer outBuffer = ByteBuffer.allocateDirect(LZ4_HIGH.maxCompressedLength(inputSize));
      closer.register(() -> ByteBufferUtils.free(outBuffer));
      return outBuffer;
    }

    @Override
    public ByteBuffer compress(ByteBuffer in, ByteBuffer out)
    {
      out.clear();
      int position = in.position();
      LZ4_HIGH.compress(in, out);
      in.position(position);
      out.flip();
      return out;
    }
  }

  /**
   * Logs info relating to whether LZ4 is using native or pure Java implementations
   */
  private static void logLZ4State()
  {
    LOG.debug("java.library.path: " + System.getProperty("java.library.path"));
    LZ4Factory fastestInstance = LZ4Factory.fastestInstance();
    try {
      //noinspection ObjectEquality
      if (fastestInstance == LZ4Factory.nativeInstance()) {
        LOG.debug("LZ4 compression is using native instance.");
      }
    }
    catch (Throwable t) {
      // getting an exception means we're not using the native instance
    }
    try {
      //noinspection ObjectEquality
      if (fastestInstance == LZ4Factory.unsafeInstance()) {
        LOG.debug("LZ4 compression is using unsafe instance.");
      }
    }
    catch (Throwable t) {
      // getting an exception means we're not using the unsafe instance
    }

    //noinspection ObjectEquality
    if (fastestInstance == LZ4Factory.safeInstance()) {
      LOG.debug("LZ4 compression is using safe instance.");
    }
  }
}
