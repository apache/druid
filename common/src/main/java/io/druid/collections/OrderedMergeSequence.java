/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.collections;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.Yielders;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingAccumulators;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * An OrderedMergeIterator is an iterator that merges together multiple sorted iterators.  It is written assuming
 * that the input Iterators are provided in order.  That is, it places an extra restriction in the input iterators.
 *
 * Normally a merge operation could operate with the actual input iterators in any order as long as the actual values
 * in the iterators are sorted.  This requires that not only the individual values be sorted, but that the iterators
 * be provided in the order of the first element of each iterator.
 *
 * If this doesn't make sense, check out OrderedMergeSequenceTest.testScrewsUpOnOutOfOrderBeginningOfList()
 *
 * It places this extra restriction on the input data in order to implement an optimization that allows it to
 * remain as lazy as possible in the face of a common case where the iterators are just appended one after the other.
 */
public class OrderedMergeSequence<T> implements Sequence<T>
{
  private final Ordering<T> ordering;
  private final Sequence<Sequence<T>> sequences;

  public OrderedMergeSequence(
      final Ordering<T> ordering,
      Sequence<Sequence<T>> sequences
  )
  {
    this.ordering = ordering;
    this.sequences = sequences;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    Yielder<OutType> yielder = null;
    try {
      yielder = toYielder(initValue, YieldingAccumulators.fromAccumulator(accumulator));
      return yielder.get();
    }
    finally {
      CloseQuietly.close(yielder);
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    PriorityQueue<Yielder<T>> pQueue = new PriorityQueue<Yielder<T>>(
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

    Yielder<Yielder<T>> oldDudeAtCrosswalk = sequences.toYielder(
        null,
        new YieldingAccumulator<Yielder<T>, Sequence<T>>()
        {
          @Override
          public Yielder<T> accumulate(Yielder<T> accumulated, Sequence<T> in)
          {
            final Yielder<T> retVal = in.toYielder(
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

            if (retVal.isDone()) {
              try {
                retVal.close();
              }
              catch (IOException e) {
                throw Throwables.propagate(e);
              }
              return null;
            }
            else {
              yield();
            }

            return retVal;
          }
        }
    );

    return makeYielder(pQueue, oldDudeAtCrosswalk, initValue, accumulator);
  }

  private <OutType> Yielder<OutType> makeYielder(
      final PriorityQueue<Yielder<T>> pQueue,
      Yielder<Yielder<T>> oldDudeAtCrosswalk,
      OutType initVal,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    OutType retVal = initVal;
    while (!accumulator.yielded() && (!pQueue.isEmpty() || !oldDudeAtCrosswalk.isDone())) {
      Yielder<T> yielder;
      if (oldDudeAtCrosswalk.isDone()) {
        yielder = pQueue.remove();
      }
      else if (pQueue.isEmpty()) {
        yielder = oldDudeAtCrosswalk.get();
        oldDudeAtCrosswalk = oldDudeAtCrosswalk.next(null);
      }
      else {
        Yielder<T> queueYielder = pQueue.peek();
        Yielder<T> iterYielder = oldDudeAtCrosswalk.get();

        if (ordering.compare(queueYielder.get(), iterYielder.get()) <= 0) {
          yielder = pQueue.remove();
        }
        else {
          yielder = oldDudeAtCrosswalk.get();
          oldDudeAtCrosswalk = oldDudeAtCrosswalk.next(null);
        }
      }

      retVal = accumulator.accumulate(retVal, yielder.get());
      yielder = yielder.next(null);
      if (yielder.isDone()) {
        try {
          yielder.close();
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
      else {
        pQueue.add(yielder);
      }
    }

    if (!accumulator.yielded()) {
      return Yielders.done(retVal, oldDudeAtCrosswalk);
    }

    final OutType yieldVal = retVal;
    final Yielder<Yielder<T>> finalOldDudeAtCrosswalk = oldDudeAtCrosswalk;
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
        return makeYielder(pQueue, finalOldDudeAtCrosswalk, initValue, accumulator);
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close() throws IOException
      {
        while(!pQueue.isEmpty()) {
          pQueue.remove().close();
        }
      }
    };
  }
}
