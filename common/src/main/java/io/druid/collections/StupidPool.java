/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.collections;

import com.google.common.base.Supplier;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class StupidPool<T>
{
  private static final Logger log = new Logger(StupidPool.class);

  private final Supplier<T> generator;

  private final Queue<T> objects = new ConcurrentLinkedQueue<>();

  public StupidPool(
      Supplier<T> generator
  )
  {
    this.generator = generator;
  }

  public ResourceHolder<T> take()
  {
    final T obj = objects.poll();
    return obj == null ? new ObjectResourceHolder(generator.get()) : new ObjectResourceHolder(obj);
  }

  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final T object;

    public ObjectResourceHolder(final T object)
    {
      this.object = object;
    }

    // WARNING: it is entirely possible for a caller to hold onto the object and call ObjectResourceHolder.close,
    // Then still use that object even though it will be offered to someone else in StupidPool.take
    @Override
    public T get()
    {
      if (closed.get()) {
        throw new ISE("Already Closed!");
      }

      return object;
    }

    @Override
    public void close() throws IOException
    {
      if (!closed.compareAndSet(false, true)) {
        log.warn(new ISE("Already Closed!"), "Already closed");
        return;
      }
      if (!objects.offer(object)) {
        log.warn(new ISE("Queue offer failed"), "Could not offer object [%s] back into the queue", object);
      }
    }

    @Override
    protected void finalize() throws Throwable
    {
      try {
        if (!closed.get()) {
          log.warn("Not closed!  Object was[%s]. Allowing gc to prevent leak.", object);
        }
      }
      finally {
        super.finalize();
      }
    }
  }
}
