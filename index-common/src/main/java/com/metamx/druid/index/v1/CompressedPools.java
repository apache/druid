package com.metamx.druid.index.v1;

import com.google.common.base.Supplier;
import com.metamx.common.logger.Logger;
import com.metamx.druid.collect.ResourceHolder;
import com.metamx.druid.collect.StupidPool;
import com.ning.compress.lzf.ChunkEncoder;

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
