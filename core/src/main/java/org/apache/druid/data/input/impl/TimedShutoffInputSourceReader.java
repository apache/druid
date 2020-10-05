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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TimedShutoffInputSourceReader implements InputSourceReader
{
  private static final Logger LOG = new Logger(TimedShutoffInputSourceReader.class);

  private final InputSourceReader delegate;
  private final DateTime shutoffTime;

  public TimedShutoffInputSourceReader(InputSourceReader delegate, DateTime shutoffTime)
  {
    this.delegate = delegate;
    this.shutoffTime = shutoffTime;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    final ScheduledExecutorService shutdownExec = Execs.scheduledSingleThreaded("timed-shutoff-reader-%d");
    final CloseableIterator<InputRow> delegateIterator = delegate.read();
    return decorateShutdownTimeout(shutdownExec, delegateIterator);
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    final ScheduledExecutorService shutdownExec = Execs.scheduledSingleThreaded("timed-shutoff-reader-%d");
    final CloseableIterator<InputRowListPlusRawValues> delegateIterator = delegate.sample();
    return decorateShutdownTimeout(shutdownExec, delegateIterator);
  }

  private <T> CloseableIterator<T> decorateShutdownTimeout(
      ScheduledExecutorService exec,
      CloseableIterator<T> delegateIterator
  )
  {
    final Closer closer = Closer.create();
    closer.register(delegateIterator);
    final CloseableIterator<T> wrappingIterator = new CloseableIterator<T>()
    {
      /**
       * Indicates this iterator has been closed or not.
       * AtomicBoolean since the value can be change by multiple concurrent calls to the {@link #close()} method
       * and we want to only allows one call.
       */
      private final AtomicBoolean closed = new AtomicBoolean(false);

      /**
       * Caching the next item. The item returned from the underling iterator is either a non-null {@link InputRow}
       * or {@link InputRowListPlusRawValues}.
       * Not volatile since {@link #hasNext()} and {@link #next()} are supposed to be called by the same thread.
       */
      T next = null;

      @Override
      public boolean hasNext()
      {
        if (next != null) {
          return true;
        }
        if (!closed.get() && delegateIterator.hasNext()) {
          next = delegateIterator.next();
          return true;
        } else {
          return false;
        }
      }

      @Override
      public T next()
      {
        if (next != null) {
          final T returnValue = next;
          next = null;
          return returnValue;
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public void close() throws IOException
      {
        if (closed.compareAndSet(false, true)) {
          closer.close();
        }
      }
    };
    exec.schedule(
        () -> {
          LOG.info("Closing dependencies due to timeout");

          try {
            wrappingIterator.close();
          }
          catch (IOException e) {
            LOG.warn(e, "Failed to close delegate inputSource, ignoring.");
          }
        },
        shutoffTime.getMillis() - System.currentTimeMillis(),
        TimeUnit.MILLISECONDS
    );
    exec.shutdown();

    return wrappingIterator;
  }
}
