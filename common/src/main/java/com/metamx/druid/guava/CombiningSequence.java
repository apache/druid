/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.guava;

import com.google.common.collect.Ordering;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.Yielders;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.nary.BinaryFn;

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
    Yielder<T> baseYielder = baseSequence.toYielder(null, combiningAccumulator);

    if (baseYielder.isDone()) {
      return Yielders.done(initValue, baseYielder);
    }

    return makeYielder(baseYielder, combiningAccumulator);
  }

  public <OutType, T> Yielder<OutType> makeYielder(
      final Yielder<T> yielder, final CombiningYieldingAccumulator<OutType, T> combiningAccumulator
  )
  {
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return combiningAccumulator.getRetVal();
      }

      @Override
      public Yielder<OutType> next(OutType outType)
      {
        T nextIn = yielder.get();
        combiningAccumulator.setRetVal(outType);
        final Yielder<T> baseYielder = yielder.next(nextIn);
        if (baseYielder.isDone()) {
          final OutType outValue = combiningAccumulator.getAccumulator().accumulate(outType, baseYielder.get());
          return Yielders.done(outValue, baseYielder);
        }
        return makeYielder(baseYielder, combiningAccumulator);
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
      if (prevValue == null) {
        return mergeFn.apply(t, null);
      }

      if (ordering.compare(prevValue, t) == 0) {
        return mergeFn.apply(prevValue, t);
      }

      retVal = accumulator.accumulate(retVal, prevValue);
      return t;
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
