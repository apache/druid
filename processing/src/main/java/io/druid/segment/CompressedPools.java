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

package io.druid.segment;

import com.google.common.base.Supplier;
import com.metamx.common.logger.Logger;
import com.ning.compress.lzf.ChunkEncoder;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class CompressedPools
{
  private static final Logger log = new Logger(CompressedPools.class);

  private static final StupidPool<ChunkEncoder> chunkEncoderPool = new StupidPool<ChunkEncoder>(
      new Supplier<ChunkEncoder>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public ChunkEncoder get()
        {
          log.info("Allocating new chunkEncoder[%,d]", counter.incrementAndGet());
          return new ChunkEncoder(0xFFFF);
        }
      }
  );

  public static ResourceHolder<ChunkEncoder> getChunkEncoder()
  {
    return chunkEncoderPool.take();
  }

  private static final StupidPool<byte[]> outputBytesPool = new StupidPool<byte[]>(
      new Supplier<byte[]>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public byte[] get()
        {
          log.info("Allocating new outputBytesPool[%,d]", counter.incrementAndGet());
          return new byte[0xFFFF];
        }
      }
  );

  public static ResourceHolder<byte[]> getOutputBytes()
  {
    return outputBytesPool.take();
  }

  private static final StupidPool<ByteBuffer> bigEndByteBufPool = new StupidPool<ByteBuffer>(
      new Supplier<ByteBuffer>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public ByteBuffer get()
        {
          log.info("Allocating new bigEndByteBuf[%,d]", counter.incrementAndGet());
          return ByteBuffer.allocateDirect(0xFFFF).order(ByteOrder.BIG_ENDIAN);
        }
      }
  );

  private static final StupidPool<ByteBuffer> littleEndByteBufPool = new StupidPool<ByteBuffer>(
      new Supplier<ByteBuffer>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public ByteBuffer get()
        {
          log.info("Allocating new littleEndByteBuf[%,d]", counter.incrementAndGet());
          return ByteBuffer.allocateDirect(0xFFFF).order(ByteOrder.LITTLE_ENDIAN);
        }
      }
  );

  public static ResourceHolder<ByteBuffer> getByteBuf(ByteOrder order)
  {
    if (order == ByteOrder.LITTLE_ENDIAN) {
      return littleEndByteBufPool.take();
    }
    return bigEndByteBufPool.take();
  }
}
