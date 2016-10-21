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

package io.druid.common.guava;

import com.google.common.collect.Ordering;

import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.Yielders;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.common.guava.nary.BinaryFn;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class CombiningSequence<T> implements Sequence<T>
{
  public static <T> CombiningSequence<T> create(
      Sequence<T> baseSequence,
      Ordering<T> ordering,
      BinaryFn<T, T, T> mergeFn
  )
  {
    return new CombiningSequence<T>(baseSequence, ordering, mergeFn);
  }

  private final Sequence<T> baseSequence;
  private final Ordering<T> ordering;
  private final BinaryFn<T, T, T> mergeFn;

  public CombiningSequence(
      Sequence<T> baseSequence,
      Ordering<T> ordering,
      BinaryFn<T, T, T> mergeFn
  )
  {
    this.baseSequence = baseSequence;
    this.ordering = ordering;
    this.mergeFn = mergeFn;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, final Accumulator<OutType, T> accumulator)
  {
    final AtomicReference<OutType> retVal = new AtomicReference<OutType>(initValue);
    final CombiningAccumulator<OutType> combiningAccumulator = new CombiningAccumulator<OutType>(retVal, accumulator);
    T lastValue = baseSequence.accumulate(null, combiningAccumulator);
    return combiningAccumulator.accumulatedSomething() ? accumulator.accumulate(retVal.get(), lastValue) : initValue;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, final YieldingAccumulator<OutType, T> accumulator)
  {
    final CombiningYieldingAccumulator<OutType, T> combiningAccumulator = new CombiningYieldingAccumulator<OutType, T>(
        ordering, mergeFn, accumulator
    );

    combiningAccumulator.setRetVal(initValue);
    Yielder<T> baseYielder = baseSequence.toYielder(null, combiningAccumulator);

    return makeYielder(baseYielder, combiningAccumulator, false);
  }

  public <OutType> Yielder<OutType> makeYielder(
      final Yielder<T> yielder,
      final CombiningYieldingAccumulator<OutType, T> combiningAccumulator,
      boolean finalValue
  )
  {
    final Yielder<T> finalYielder;
    final OutType retVal;
    final boolean finalFinalValue;

    if(!yielder.isDone()) {
      retVal = combiningAccumulator.getRetVal();
      finalYielder = null;
      finalFinalValue = false;
    } else {
      if(!finalValue && combiningAccumulator.accumulatedSomething()) {
        combiningAccumulator.accumulateLastValue();
        retVal = combiningAccumulator.getRetVal();
        finalFinalValue = true;

        if(!combiningAccumulator.yielded()) {
          return Yielders.done(retVal, yielder);
        } else {
          finalYielder = Yielders.done(null, yielder);
        }
      }
      else {
        return Yielders.done(combiningAccumulator.getRetVal(), yielder);
      }
    }


    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return retVal;
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        combiningAccumulator.reset();
        return makeYielder(
            finalYielder == null ? yielder.next(yielder.get()) : finalYielder,
            combiningAccumulator,
            finalFinalValue
        );
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close() throws IOException
      {
        yielder.close();
      }
    };
  }

  private static class CombiningYieldingAccumulator<OutType, T> extends YieldingAccumulator<T, T>
  {
    private final Ordering<T> ordering;
    private final BinaryFn<T, T, T> mergeFn;
    private final YieldingAccumulator<OutType, T> accumulator;

    private volatile OutType retVal;
    private volatile T lastMergedVal;
    private volatile boolean accumulatedSomething = false;

    public CombiningYieldingAccumulator(
        Ordering<T> ordering,
        BinaryFn<T, T, T> mergeFn,
        YieldingAccumulator<OutType, T> accumulator
    )
    {
      this.ordering = ordering;
      this.mergeFn = mergeFn;
      this.accumulator = accumulator;
    }

    public OutType getRetVal()
    {
      return retVal;
    }

    public void setRetVal(OutType retVal)
    {
      this.retVal = retVal;
    }

    public YieldingAccumulator<OutType, T> getAccumulator()
    {
      return accumulator;
    }

    public void reset()
    {
      accumulator.reset();
    }

    public boolean yielded()
    {
      return accumulator.yielded();
    }

    public void yield()
    {
      accumulator.yield();
    }

    @Override
    public T accumulate(T prevValue, T t)
    {
      if (!accumulatedSomething) {
        accumulatedSomething = true;
      }

      if (prevValue == null) {
        lastMergedVal = mergeFn.apply(t, null);
        return lastMergedVal;
      }

      if (ordering.compare(prevValue, t) == 0) {
        lastMergedVal = mergeFn.apply(prevValue, t);
        return lastMergedVal;
      }

      lastMergedVal = t;
      retVal = accumulator.accumulate(retVal, prevValue);
      return t;
    }

    public void accumulateLastValue()
    {
      retVal = accumulator.accumulate(retVal, lastMergedVal);
    }

    public boolean accumulatedSomething()
    {
      return accumulatedSomething;
    }
  }

  private class CombiningAccumulator<OutType> implements Accumulator<T, T>
  {
    private final AtomicReference<OutType> retVal;
    private final Accumulator<OutType, T> accumulator;

    private volatile boolean accumulatedSomething = false;

    public CombiningAccumulator(AtomicReference<OutType> retVal, Accumulator<OutType, T> accumulator)
    {
      this.retVal = retVal;
      this.accumulator = accumulator;
    }

    public boolean accumulatedSomething()
    {
      return accumulatedSomething;
    }

    @Override
    public T accumulate(T prevValue, T t)
    {
      if (!accumulatedSomething) {
        accumulatedSomething = true;
      }

      if (prevValue == null) {
        return mergeFn.apply(t, null);
      }

      if (ordering.compare(prevValue, t) == 0) {
        return mergeFn.apply(prevValue, t);
      }

      retVal.set(accumulator.accumulate(retVal.get(), prevValue));
      return t;
    }
  }
}
