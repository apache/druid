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

import org.apache.druid.java.util.common.IAE;

import java.io.IOException;

/**
 * A Sequence that skips the first few elements.
 */
public class SkippingSequence<T> extends YieldingSequenceBase<T>
{
  private final Sequence<T> baseSequence;
  private final long skip;

  public SkippingSequence(Sequence<T> baseSequence, long skip)
  {
    this.baseSequence = baseSequence;
    this.skip = skip;

    if (skip < 1) {
      throw new IAE("'skip' must be greater than zero");
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final SkippingYieldingAccumulator<OutType> skippingAccumulator = new SkippingYieldingAccumulator<>(accumulator);
    return wrapYielder(baseSequence.toYielder(initValue, skippingAccumulator), skippingAccumulator);
  }

  private <OutType> Yielder<OutType> wrapYielder(
      final Yielder<OutType> yielder,
      final SkippingYieldingAccumulator<OutType> accumulator
  )
  {
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return yielder.get();
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        return wrapYielder(yielder.next(initValue), accumulator);
      }

      @Override
      public boolean isDone()
      {
        return yielder.isDone();
      }

      @Override
      public void close() throws IOException
      {
        yielder.close();
      }
    };
  }

  private class SkippingYieldingAccumulator<OutType> extends DelegatingYieldingAccumulator<OutType, T>
  {
    private long skipped = 0;

    public SkippingYieldingAccumulator(final YieldingAccumulator<OutType, T> accumulator)
    {
      super(accumulator);
    }

    @Override
    public OutType accumulate(OutType accumulated, T in)
    {
      if (skipped < skip) {
        skipped++;
        return accumulated;
      } else {
        return super.accumulate(accumulated, in);
      }
    }
  }
}
