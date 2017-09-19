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

package io.druid.segment;

import com.google.common.base.Preconditions;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ReferenceCountingSegment allows to call {@link #close()} before some other "users", which called {@link
 * #increment()}, has not called {@link #decrement()} yet, and the wrapped {@link Segment} won't be actually closed
 * until that. So ReferenceCountingSegment implements something like automatic reference count-based resource
 * management.
 */
public class ReferenceCountingSegment extends AbstractSegment
{
  private static final EmittingLogger log = new EmittingLogger(ReferenceCountingSegment.class);

  private final Segment baseSegment;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Phaser referents = new Phaser(1)
  {
    @Override
    protected boolean onAdvance(int phase, int registeredParties)
    {
      Preconditions.checkState(registeredParties == 0);
      // Ensure that onAdvance() doesn't throw exception, otherwise termination won't happen
      try {
        baseSegment.close();
      }
      catch (Exception e) {
        try {
          log.error(e, "Exception while closing segment[%s]", baseSegment.getIdentifier());
        }
        catch (Exception e2) {
          // ignore
        }
      }
      // Always terminate.
      return true;
    }
  };

  public ReferenceCountingSegment(Segment baseSegment)
  {
    this.baseSegment = baseSegment;
  }

  public Segment getBaseSegment()
  {
    return !isClosed() ? baseSegment : null;
  }

  public int getNumReferences()
  {
    return Math.max(referents.getRegisteredParties() - 1, 0);
  }

  public boolean isClosed()
  {
    return referents.isTerminated();
  }

  @Override
  public String getIdentifier()
  {
    return !isClosed() ? baseSegment.getIdentifier() : null;
  }

  @Override
  public Interval getDataInterval()
  {
    return !isClosed() ? baseSegment.getDataInterval() : null;
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    return !isClosed() ? baseSegment.asQueryableIndex() : null;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return !isClosed() ? baseSegment.asStorageAdapter() : null;
  }

  @Override
  public void close()
  {
    if (closed.compareAndSet(false, true)) {
      referents.arriveAndDeregister();
    } else {
      log.warn("close() is called more than once on ReferenceCountingSegment");
    }
  }

  public boolean increment()
  {
    // Negative return from referents.register() means the Phaser is terminated.
    return referents.register() >= 0;
  }

  /**
   * Returns a {@link Closeable} which action is to call {@link #decrement()} only once. If close() is called on the
   * returned Closeable object for the second time, it won't call {@link #decrement()} again.
   */
  public Closeable decrementOnceCloseable()
  {
    AtomicBoolean decremented = new AtomicBoolean(false);
    return () -> {
      if (decremented.compareAndSet(false, true)) {
        decrement();
      } else {
        log.warn("close() is called more than once on ReferenceCountingSegment.decrementOnceCloseable()");
      }
    };
  }

  public void decrement()
  {
    referents.arriveAndDeregister();
  }

  @Override
  public <T> T as(Class<T> clazz)
  {
    return getBaseSegment().as(clazz);
  }
}
