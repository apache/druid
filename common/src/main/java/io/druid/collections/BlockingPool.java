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

package io.druid.collections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import io.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Pool that pre-generates objects up to a limit, then permits possibly-blocking "take" operations.
 */
public class BlockingPool<T>
{
  private static final Logger log = new Logger(BlockingPool.class);

  private final BlockingQueue<T> objects;
  private final int maxSize;

  public BlockingPool(
      Supplier<T> generator,
      int limit
  )
  {
    this.objects = limit > 0 ? new ArrayBlockingQueue<T>(limit) : null;
    this.maxSize = limit;

    for (int i = 0; i < limit; i++) {
      objects.add(generator.get());
    }
  }

  public int maxSize()
  {
    return maxSize;
  }

  @VisibleForTesting
  public int getPoolSize()
  {
    return objects.size();
  }

  /**
   * Take a resource from the pool.
   *
   * @param timeout maximum time to wait for a resource, in milliseconds. Negative means do not use a timeout.
   *
   * @return a resource, or null if the timeout was reached
   */
  public ReferenceCountingResourceHolder<T> take(final long timeout)
  {
    checkInitialized();
    final T theObject;
    try {
      if (timeout > -1) {
        theObject = timeout > 0 ? objects.poll(timeout, TimeUnit.MILLISECONDS) : objects.poll();
      } else {
        theObject = objects.take();
      }
      return theObject == null ? null : new ReferenceCountingResourceHolder<>(
          theObject,
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              offer(theObject);
            }
          }
      );
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Drains at most the given number of available resources from the pool.
   *
   * @param maxElements the maximum number of elements to drain
   * @param timeout     maximum time to wait for a resource, in milliseconds. Negative means do not use a timeout.
   *
   * @return a resource holder which contains the drained resources
   */
  public ReferenceCountingResourceHolder<List<T>> drain(final int maxElements, final long timeout)
  {
    checkInitialized();
    final List<T> batch = Lists.newArrayListWithCapacity(maxElements);

    try {
      final int n = timeout > 0 ?
                    Queues.drain(objects, batch, maxElements, timeout, TimeUnit.MILLISECONDS) :
                    objects.drainTo(batch, maxElements);
      if (n < maxElements) {
        log.debug("Requested %d elements, but drained %d elements", maxElements, n);
      }
    }
    catch (InterruptedException e) {
      for (T obj : batch) {
        offer(obj);
      }
      throw Throwables.propagate(e);
    }

    final List<T> resources = ImmutableList.copyOf(batch);
    return new ReferenceCountingResourceHolder<>(
        resources,
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            for (T obj : resources) {
              offer(obj);
            }
          }
        }
    );
  }

  private void checkInitialized()
  {
    Preconditions.checkState(objects != null, "Pool was initialized with limit = 0, there are no objects to take.");
  }

  private void offer(T theObject)
  {
    if (!objects.offer(theObject)) {
      log.error("WTF?! Queue offer failed, uh oh...");
    }
  }
}
