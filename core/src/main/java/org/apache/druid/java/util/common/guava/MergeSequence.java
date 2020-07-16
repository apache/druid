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

package org.apache.druid.java.util.common.guava;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.io.Closer;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Used to perform an n-way merge on n ordered sequences
 */
public class MergeSequence<T> extends YieldingSequenceBase<T>
{
  private final Ordering<? super T> ordering;
  private final Sequence<? extends Sequence<T>> baseSequences;

  public MergeSequence(
      Ordering<? super T> ordering,
      Sequence<? extends Sequence<? extends T>> baseSequences
  )
  {
    this.ordering = ordering;
    this.baseSequences = (Sequence<? extends Sequence<T>>) baseSequences;
  }

  /*
    Note: the yielder for MergeSequence returns elements from the priority queue in order of increasing priority.
    This is due to the fact that PriorityQueue#remove() polls from the head of the queue which is, according to
    the PriorityQueue javadoc, "the least element with respect to the specified ordering"
   */
  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    PriorityQueue<Yielder<T>> pQueue = new PriorityQueue<>(
        32,
        ordering.onResultOf(
            (Function<Yielder<T>, T>) input -> input.get()
        )
    );

    try {
      pQueue = baseSequences.accumulate(
          pQueue,
          (queue, in) -> {
            final Yielder<T> yielder = in.toYielder(
                null,
                new YieldingAccumulator<T, T>()
                {
                  @Override
                  public T accumulate(T accumulated, T in)
                  {
                    yield();
                    return in;
                  }
                }
            );

            if (!yielder.isDone()) {
              try {
                queue.add(yielder);
              }
              catch (Throwable t1) {
                try {
                  yielder.close();
                }
                catch (Throwable t2) {
                  t1.addSuppressed(t2);
                }

                throw t1;
              }
            } else {
              try {
                yielder.close();
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            return queue;
          }
      );

      return makeYielder(pQueue, initValue, accumulator);
    }
    catch (Throwable t1) {
      try {
        closeAll(pQueue);
      }
      catch (Throwable t2) {
        t1.addSuppressed(t2);
      }

      throw t1;
    }
  }

  private <OutType> Yielder<OutType> makeYielder(
      final PriorityQueue<Yielder<T>> pQueue,
      OutType initVal,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    OutType retVal = initVal;
    while (!accumulator.yielded() && !pQueue.isEmpty()) {
      Yielder<T> yielder = pQueue.remove();

      try {
        retVal = accumulator.accumulate(retVal, yielder.get());
        yielder = yielder.next(null);
      }
      catch (Throwable t1) {
        try {
          yielder.close();
        }
        catch (Throwable t2) {
          t1.addSuppressed(t2);
        }

        throw t1;
      }

      if (yielder.isDone()) {
        try {
          yielder.close();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        pQueue.add(yielder);
      }
    }

    if (pQueue.isEmpty() && !accumulator.yielded()) {
      return Yielders.done(retVal, null);
    }

    final OutType yieldVal = retVal;
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return yieldVal;
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        accumulator.reset();
        return makeYielder(pQueue, initValue, accumulator);
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close() throws IOException
      {
        closeAll(pQueue);
      }
    };
  }

  private static <T> void closeAll(final PriorityQueue<Yielder<T>> pQueue) throws IOException
  {
    Closer closer = Closer.create();
    while (!pQueue.isEmpty()) {
      final Yielder<T> yielder = pQueue.poll();
      if (yielder != null) {
        // Note: yielder can be null if our comparator threw an exception during queue.add.
        closer.register(yielder);
      }
    }
    closer.close();
  }
}
