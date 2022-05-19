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

package org.apache.druid.segment;

import com.google.common.base.Supplier;
import com.ning.compress.BufferRecycler;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

public class CompressedPools
{
  private static final Logger log = new Logger(CompressedPools.class);

  public static final int BUFFER_SIZE = 0x10000;
  private static final NonBlockingPool<BufferRecycler> BUFFER_RECYCLER_POOL = new StupidPool<>(
      "bufferRecyclerPool",
      new Supplier<BufferRecycler>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public BufferRecycler get()
        {
          log.debug("Allocating new bufferRecycler[%,d]", counter.incrementAndGet());
          return new BufferRecycler();
        }
      }
  );

  public static ResourceHolder<BufferRecycler> getBufferRecycler()
  {
    return BUFFER_RECYCLER_POOL.take();
  }

  private static final NonBlockingPool<byte[]> OUTPUT_BYTES_POOL = new StupidPool<byte[]>(
      "outputBytesPool",
      new Supplier<byte[]>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public byte[] get()
        {
          log.debug("Allocating new outputBytesPool[%,d]", counter.incrementAndGet());
          return new byte[BUFFER_SIZE];
        }
      }
  );

  public static ResourceHolder<byte[]> getOutputBytes()
  {
    return OUTPUT_BYTES_POOL.take();
  }

  private static final NonBlockingPool<ByteBuffer> BIG_ENDIAN_BYTE_BUF_POOL = new StupidPool<ByteBuffer>(
      "bigEndByteBufPool",
      new Supplier<ByteBuffer>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public ByteBuffer get()
        {
          log.debug("Allocating new bigEndByteBuf[%,d]", counter.incrementAndGet());
          return ByteBuffer.allocateDirect(BUFFER_SIZE).order(ByteOrder.BIG_ENDIAN);
        }
      }
  );

  private static final NonBlockingPool<ByteBuffer> LITTLE_ENDIAN_BYTE_BUF_POOL = new StupidPool<ByteBuffer>(
      "littleEndByteBufPool",
      new Supplier<ByteBuffer>()
      {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public ByteBuffer get()
        {
          log.debug("Allocating new littleEndByteBuf[%,d]", counter.incrementAndGet());
          return ByteBuffer.allocateDirect(BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        }
      }
  );

  public static ResourceHolder<ByteBuffer> getByteBuf(ByteOrder order)
  {
    if (order == ByteOrder.LITTLE_ENDIAN) {
      return LITTLE_ENDIAN_BYTE_BUF_POOL.take();
    }
    return BIG_ENDIAN_BYTE_BUF_POOL.take();
  }
}
