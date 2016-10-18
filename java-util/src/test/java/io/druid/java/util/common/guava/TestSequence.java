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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
*/
public class TestSequence<T> implements Sequence<T>
{
  public static <T> TestSequence<T> create(Iterable<T> iterable)
  {
    return new TestSequence<>(iterable);
  }

  public static <T> TestSequence<T> create(T... vals)
  {
    return create(Arrays.asList(vals));
  }

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Sequence<T> base;

  public TestSequence(final Iterable<T> iterable)
  {
    base = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
    {
      @Override
      public Iterator<T> make()
      {
        return iterable.iterator();
      }

      @Override
      public void cleanup(Iterator<T> iterFromMake)
      {
        closed.set(true);
      }
    });
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    return base.accumulate(initValue, accumulator);
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    return base.toYielder(initValue, accumulator);
  }

  public boolean isClosed()
  {
    return closed.get();
  }
}
