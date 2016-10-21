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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

public class ReferenceCountingResourceHolder<T> implements ResourceHolder<T>
{
  private static final Logger log = new Logger(ReferenceCountingResourceHolder.class);

  private final Object lock = new Object();
  private final T object;
  private final Closeable closer;

  private int refcount = 1;
  private boolean didClose = false;

  public ReferenceCountingResourceHolder(final T object, final Closeable closer)
  {
    this.object = object;
    this.closer = closer;
  }

  public static <T extends Closeable> ReferenceCountingResourceHolder<T> fromCloseable(final T object)
  {
    return new ReferenceCountingResourceHolder<>(object, object);
  }

  @Override
  public T get()
  {
    synchronized (lock) {
      if (refcount <= 0) {
        throw new ISE("Already closed!");
      }

      return object;
    }
  }

  public Releaser increment()
  {
    synchronized (lock) {
      if (refcount <= 0) {
        throw new ISE("Already closed!");
      }

      refcount++;

      return new Releaser()
      {
        final AtomicBoolean didRelease = new AtomicBoolean();

        @Override
        public void close()
        {
          if (didRelease.compareAndSet(false, true)) {
            decrement();
          } else {
            log.warn("WTF?! release called but we are already released!");
          }
        }

        @Override
        protected void finalize() throws Throwable
        {
          if (didRelease.compareAndSet(false, true)) {
            log.warn("Not released! Object was[%s], releasing on finalize of releaser.", object);
            decrement();
          }
        }
      };
    }
  }

  public int getReferenceCount()
  {
    synchronized (lock) {
      return refcount;
    }
  }

  @Override
  public void close()
  {
    synchronized (lock) {
      if (!didClose) {
        didClose = true;
        decrement();
      } else {
        log.warn(new ISE("Already closed!"), "Already closed");
      }
    }
  }

  @Override
  protected void finalize() throws Throwable
  {
    synchronized (lock) {
      if (!didClose) {
        log.warn("Not closed! Object was[%s], closing on finalize of holder.", object);
        didClose = true;
        decrement();
      }
    }
  }

  private void decrement()
  {
    synchronized (lock) {
      refcount--;
      if (refcount <= 0) {
        try {
          closer.close();
        }
        catch (Exception e) {
          log.error(e, "WTF?! Close failed, uh oh...");
        }
      }
    }
  }
}
