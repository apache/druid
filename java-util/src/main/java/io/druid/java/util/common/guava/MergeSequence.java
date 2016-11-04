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

package io.druid.java.util.common.guava;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 */
public class MergeSequence<T> extends YieldingSequenceBase<T>
{
  private final Ordering<T> ordering;
  private final Sequence<Sequence<T>> baseSequences;

  public MergeSequence(
      Ordering<T> ordering,
      Sequence<Sequence<T>> baseSequences
  )
  {
    this.ordering = ordering;
    this.baseSequences = baseSequences;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    PriorityQueue<Yielder<T>> pQueue = new PriorityQueue<>(
        32,
        ordering.onResultOf(
            new Function<Yielder<T>, T>()
            {
              @Override
              public T apply(Yielder<T> input)
              {
                return input.get();
              }
            }
        )
    );

    pQueue = baseSequences.accumulate(
        pQueue,
        new Accumulator<PriorityQueue<Yielder<T>>, Sequence<T>>()
        {
          @Override
          public PriorityQueue<Yielder<T>> accumulate(PriorityQueue<Yielder<T>> queue, Sequence<T> in)
          {
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
              queue.add(yielder);
            } else {
              try {
                yielder.close();
              }
              catch (IOException e) {
                throw Throwables.propagate(e);
              }
            }

            return queue;
          }
        }
    );

    return makeYielder(pQueue, initValue, accumulator);
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
      retVal = accumulator.accumulate(retVal, yielder.get());
      yielder = yielder.next(null);
      if (yielder.isDone()) {
        try {
          yielder.close();
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
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
        while (!pQueue.isEmpty()) {
          pQueue.remove().close();
        }
      }
    };
  }
}
