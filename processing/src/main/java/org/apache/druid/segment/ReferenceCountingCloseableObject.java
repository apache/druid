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

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ReferenceCountingCloseableObject implements something like automatic reference count-based resource management,
 * backed by a {@link Phaser}.
 *
 * ReferenceCountingCloseableObject allows consumers to call {@link #close()} before some other "users", which called
 * {@link #increment()} or {@link #incrementReferenceAndDecrementOnceCloseable()}, but have not called
 * {@link #decrement()} yet or the closer for {@link #incrementReferenceAndDecrementOnceCloseable()}, and the wrapped
 * object won't be actually closed until that all references are released.
 */
public abstract class ReferenceCountingCloseableObject<BaseObject extends Closeable> implements Closeable
{
  private static final Logger log = new Logger(ReferenceCountingCloseableObject.class);

  private final AtomicBoolean closed = new AtomicBoolean(false);
  protected final Phaser referents = new Phaser(1)
  {
    @Override
    protected boolean onAdvance(int phase, int registeredParties)
    {
      // Ensure that onAdvance() doesn't throw exception, otherwise termination won't happen
      if (registeredParties != 0) {
        log.error("registeredParties[%s] is not 0", registeredParties);
      }
      try {
        baseObject.close();
      }
      catch (Exception e) {
        try {
          log.error(e, "Exception while closing reference counted object[%s]", baseObject);
        }
        catch (Exception e2) {
          // ignore
        }
      }
      // Always terminate.
      return true;
    }
  };

  protected final BaseObject baseObject;

  public ReferenceCountingCloseableObject(BaseObject object)
  {
    this.baseObject = object;
  }

  public int getNumReferences()
  {
    return Math.max(referents.getRegisteredParties() - 1, 0);
  }

  public boolean isClosed()
  {
    return referents.isTerminated();
  }

  /**
   * Increment the reference count by one.
   */
  public boolean increment()
  {
    // Negative return from referents.register() means the Phaser is terminated.
    return referents.register() >= 0;
  }

  /**
   * Decrement the reference count by one.
   */
  public void decrement()
  {
    referents.arriveAndDeregister();
  }

  /**
   * Returns an {@link Optional} of a {@link Closeable} from {@link #decrementOnceCloseable}, if it is able to
   * successfully {@link #increment}, else nothing indicating that the reference could not be acquired.
   */
  public Optional<Closeable> incrementReferenceAndDecrementOnceCloseable()
  {
    final Closer closer;
    if (increment()) {
      closer = Closer.create();
      closer.register(decrementOnceCloseable());
    } else {
      closer = null;
    }
    return Optional.ofNullable(closer);
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
        log.warn("close() is called more than once on ReferenceCountingCloseableObject.decrementOnceCloseable()");
      }
    };
  }

  @Override
  public void close()
  {
    if (closed.compareAndSet(false, true)) {
      referents.arriveAndDeregister();
    } else {
      log.warn("close() is called more than once on ReferenceCountingCloseableObject");
    }
  }
}
