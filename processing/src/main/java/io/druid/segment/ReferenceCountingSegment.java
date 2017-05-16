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

import com.metamx.emitter.EmittingLogger;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReferenceCountingSegment extends AbstractSegment
{
  private static final EmittingLogger log = new EmittingLogger(ReferenceCountingSegment.class);

  private final Segment baseSegment;

  private final Object lock = new Object();

  private volatile int numReferences = 0;
  private volatile boolean isClosed = false;

  public ReferenceCountingSegment(Segment baseSegment)
  {
    this.baseSegment = baseSegment;
  }

  public Segment getBaseSegment()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }

      return baseSegment;
    }
  }

  public int getNumReferences()
  {
    return numReferences;
  }

  public boolean isClosed()
  {
    return isClosed;
  }

  @Override
  public String getIdentifier()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }

      return baseSegment.getIdentifier();
    }
  }

  @Override
  public Interval getDataInterval()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }

      return baseSegment.getDataInterval();
    }
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }

      return baseSegment.asQueryableIndex();
    }
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }

      return baseSegment.asStorageAdapter();
    }
  }

  @Override
  public void close() throws IOException
  {
    synchronized (lock) {
      if (isClosed) {
        log.info("Failed to close, %s is closed already", baseSegment.getIdentifier());
        return;
      }

      if (numReferences > 0) {
        log.info("%d references to %s still exist. Decrementing.", numReferences, baseSegment.getIdentifier());

        decrement();
      } else {
        log.info("Closing %s", baseSegment.getIdentifier());
        innerClose();
      }
    }
  }

  public Closeable increment()
  {
    synchronized (lock) {
      if (isClosed) {
        return null;
      }

      numReferences++;
      final AtomicBoolean decrementOnce = new AtomicBoolean(false);
      return new Closeable()
      {
        @Override
        public void close() throws IOException
        {
          if (decrementOnce.compareAndSet(false, true)) {
            decrement();
          }
        }
      };
    }
  }

  private void decrement()
  {
    synchronized (lock) {
      if (isClosed) {
        return;
      }

      if (--numReferences < 0) {
        try {
          innerClose();
        }
        catch (Exception e) {
          log.error("Unable to close queryable index %s", getIdentifier());
        }
      }
    }
  }

  private void innerClose() throws IOException
  {
    synchronized (lock) {
      log.info("Closing %s, numReferences: %d", baseSegment.getIdentifier(), numReferences);

      isClosed = true;
      baseSegment.close();
    }
  }

  @Override
  public <T> T as(Class<T> clazz)
  {
    return getBaseSegment().as(clazz);
  }
}
