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

package io.druid.offheap;

import com.google.common.base.Supplier;
import com.metamx.common.logger.Logger;
import io.druid.collections.StupidPool;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;


public class OffheapBufferPool extends StupidPool<ByteBuffer>
{
  private static final Logger log = new Logger(OffheapBufferPool.class);

  public OffheapBufferPool(final int computationBufferSize, final int cacheMaxCount)
  {
    super(
        new Supplier<ByteBuffer>()
        {
          final AtomicLong count = new AtomicLong(0);

          @Override
          public ByteBuffer get()
          {
            log.info(
                "Allocating new intermediate processing buffer[%,d] of size[%,d]",
                count.getAndIncrement(), computationBufferSize
            );
            return ByteBuffer.allocateDirect(computationBufferSize);
          }
        },
        cacheMaxCount
    );
  }
}
