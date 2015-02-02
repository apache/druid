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
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.util.LinkedList;

/**
 */
public class StupidPool<T>
{
  private static final Logger log = new Logger(StupidPool.class);

  private final Supplier<T> generator;

  private final LinkedList<T> objects = Lists.newLinkedList();

  public StupidPool(
      Supplier<T> generator
  )
  {
    this.generator = generator;
  }

  public ResourceHolder<T> take()
  {
    synchronized (objects) {
      if (objects.size() > 0) {
        return new ObjectResourceHolder(objects.removeFirst());
      }
    }

    return new ObjectResourceHolder(generator.get());
  }

  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private boolean closed = false;
    private final T object;

    public ObjectResourceHolder(final T object)
    {
      this.object = object;
    }

    @Override
    public synchronized T get()
    {
      if (closed) {
        throw new ISE("Already Closed!");
      }

      return object;
    }

    @Override
    public synchronized void close() throws IOException
    {
      if (closed) {
        log.warn(new ISE("Already Closed!"), "Already closed");
        return;
      }

      synchronized (objects) {
        closed = true;
        objects.addLast(object);
      }
    }

    @Override
    protected void finalize() throws Throwable
    {
      if (! closed) {
        log.warn("Not closed!  Object was[%s]. Allowing gc to prevent leak.", object);
      }
    }
  }
}
