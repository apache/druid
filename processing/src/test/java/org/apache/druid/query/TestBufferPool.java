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

package org.apache.druid.query;

import com.google.common.collect.Iterables;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.utils.CloseableUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A buffer pool that throws away buffers when they are "returned" to the pool. Useful for tests that need to make
 * many pools and use them one at a time.
 * <p>
 * This pool implements {@link BlockingPool}, but never blocks. It returns immediately if resources are available;
 * otherwise it returns an empty list immediately. This is also useful for tests, because it allows "timeouts" to
 * happen immediately and therefore speeds up tests.
 */
public class TestBufferPool implements NonBlockingPool<ByteBuffer>, BlockingPool<ByteBuffer>
{
  private final AtomicLong takeCount = new AtomicLong(0);
  private final ConcurrentHashMap<Long, RuntimeException> takenFromMap = new ConcurrentHashMap<>();

  private final Supplier<ResourceHolder<ByteBuffer>> generator;
  private final int maxCount;

  private TestBufferPool(final Supplier<ResourceHolder<ByteBuffer>> generator, final int maxCount)
  {
    this.generator = generator;
    this.maxCount = maxCount;
  }

  public static TestBufferPool onHeap(final int bufferSize, final int maxCount)
  {
    return new TestBufferPool(
        () -> new ReferenceCountingResourceHolder<>(ByteBuffer.allocate(bufferSize), () -> {
        }),
        maxCount
    );
  }

  public static TestBufferPool offHeap(final int bufferSize, final int maxCount)
  {
    return new TestBufferPool(
        () -> ByteBufferUtils.allocateDirect(bufferSize),
        maxCount
    );
  }


  @Override
  public int maxSize()
  {
    return maxCount;
  }

  @Override
  public ResourceHolder<ByteBuffer> take()
  {
    final List<ReferenceCountingResourceHolder<ByteBuffer>> holders = takeBatch(1);

    if (holders.isEmpty()) {
      throw new ISE("Too many objects outstanding");
    } else {
      return Iterables.getOnlyElement(holders);
    }
  }

  @Override
  public List<ReferenceCountingResourceHolder<ByteBuffer>> takeBatch(int elementNum, long timeoutMs)
  {
    return takeBatch(elementNum);
  }

  @Override
  public List<ReferenceCountingResourceHolder<ByteBuffer>> takeBatch(int elementNum)
  {
    synchronized (this) {
      if (takenFromMap.size() + elementNum <= maxCount) {
        final List<ReferenceCountingResourceHolder<ByteBuffer>> retVal = new ArrayList<>();

        try {
          for (int i = 0; i < elementNum; i++) {
            final ResourceHolder<ByteBuffer> holder = generator.get();
            final ByteBuffer o = holder.get();
            final long ticker = takeCount.getAndIncrement();
            takenFromMap.put(ticker, new RuntimeException());

            retVal.add(new ReferenceCountingResourceHolder<>(o, () -> {
              takenFromMap.remove(ticker);
              holder.close();
            }));
          }
        }
        catch (Throwable e) {
          throw CloseableUtils.closeAndWrapInCatch(e, () -> CloseableUtils.closeAll(retVal));
        }

        return retVal;
      } else {
        return Collections.emptyList();
      }
    }
  }

  public long getOutstandingObjectCount()
  {
    return takenFromMap.size();
  }

  public Collection<RuntimeException> getOutstandingExceptionsCreated()
  {
    return takenFromMap.values();
  }
}
