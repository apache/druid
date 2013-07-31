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

package com.metamx.druid.index;

import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingSequenceBase;

import java.io.IOException;

/**
 */
public class ReferenceCountingSequence<T> extends YieldingSequenceBase<T>
{
  private final Sequence<T> baseSequence;
  private final ReferenceCountingSegment segment;

  public ReferenceCountingSequence(Sequence<T> baseSequence, ReferenceCountingSegment segment)
  {
    this.baseSequence = baseSequence;
    this.segment = segment;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue, YieldingAccumulator<OutType, T> accumulator
  )
  {
    segment.increment();
    return new ReferenceCountingYielder<OutType>(baseSequence.toYielder(initValue, accumulator), segment);
  }

  private static class ReferenceCountingYielder<OutType> implements Yielder<OutType>
  {
    private final Yielder<OutType> baseYielder;
    private final ReferenceCountingSegment segment;

    public ReferenceCountingYielder(Yielder<OutType> baseYielder, ReferenceCountingSegment segment)
    {
      this.baseYielder = baseYielder;
      this.segment = segment;
    }

    @Override
    public OutType get()
    {
      return baseYielder.get();
    }

    @Override
    public Yielder<OutType> next(OutType initValue)
    {
      return new ReferenceCountingYielder<OutType>(baseYielder.next(initValue), segment);
    }

    @Override
    public boolean isDone()
    {
      return baseYielder.isDone();
    }

    @Override
    public void close() throws IOException
    {
      segment.decrement();
      baseYielder.close();
    }
  }
}