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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pool that pre-generates objects up to a limit, then permits possibly-blocking "take" operations.
 */
public class BlockingPool<T>
{
  private static final Logger log = new Logger(BlockingPool.class);

  private final BlockingQueue<T> objects;

  public BlockingPool(
      Supplier<T> generator,
      int limit
  )
  {
    this.objects = limit > 0 ? new ArrayBlockingQueue<T>(limit) : null;

    for (int i = 0; i < limit; i++) {
      objects.add(generator.get());
    }
  }

  /**
   * Take a resource from the pool.
   *
   * @param timeout maximum time to wait for a resource, in milliseconds. Negative means do not use a timeout.
   *
   * @return a resource, or null if the timeout was reached
   *
   * @throws InterruptedException if interrupted while waiting for a resource to become available
   */
  public ResourceHolder<T> take(final long timeout) throws InterruptedException
  {
    Preconditions.checkState(objects != null, "Pool was initialized with limit = 0, there are no objects to take.");
    final T theObject = timeout >= 0 ? objects.poll(timeout, TimeUnit.MILLISECONDS) : objects.take();
    return theObject == null ? null : new ObjectResourceHolder(theObject);
  }

  /**
   * Similar to StupidPool.ObjectResourceHolder, except this one has no objectsCacheMaxCount, and it returns objects
   * to the pool on finalize.
   */
  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final T object;

    public ObjectResourceHolder(final T object)
    {
      this.object = object;
    }

    // WARNING: it is entirely possible for a caller to hold onto the object and call "close", then still use that
    // object even though it will be offered to someone else in BlockingPool.take
    @Override
    public T get()
    {
      if (closed.get()) {
        throw new ISE("Already Closed!");
      }

      return object;
    }

    @Override
    public void close()
    {
      if (!closed.compareAndSet(false, true)) {
        log.warn(new ISE("Already Closed!"), "Already closed");
        return;
      }
      if (!objects.offer(object)) {
        throw new ISE("WTF?! Queue offer failed");
      }
    }

    @Override
    protected void finalize() throws Throwable
    {
      if (closed.compareAndSet(false, true)) {
        log.warn("Not closed! Object was[%s]. Returning to pool.", object);
        if (!objects.offer(object)) {
          log.error("WTF?! Queue offer failed during finalize, uh oh...");
        }
      }
    }
  }
}
