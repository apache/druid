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
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;

/**
 */
public class Sequences
{

  private static final EmptySequence EMPTY_SEQUENCE = new EmptySequence();

  public static <T> Sequence<T> simple(final Iterable<T> iterable)
  {
    return BaseSequence.simple(iterable);
  }

  @SuppressWarnings("unchecked")
  public static <T> Sequence<T> empty()
  {
    return (Sequence<T>) EMPTY_SEQUENCE;
  }

  public static <T> Sequence<T> concat(Sequence<T>... sequences)
  {
    return concat(Arrays.asList(sequences));
  }

  public static <T> Sequence<T> concat(Iterable<Sequence<T>> sequences)
  {
    return concat(Sequences.simple(sequences));
  }

  public static <T> Sequence<T> concat(Sequence<Sequence<T>> sequences)
  {
    return new ConcatSequence<>(sequences);
  }

  public static <From, To> Sequence<To> map(Sequence<From> sequence, Function<From, To> fn)
  {
    return new MappedSequence<>(sequence, fn);
  }

  public static <T> Sequence<T> filter(Sequence<T> sequence, Predicate<T> pred)
  {
    return new FilteredSequence<>(sequence, pred);
  }

  public static <T> Sequence<T> limit(final Sequence<T> sequence, final int limit)
  {
    return new LimitedSequence<>(sequence, limit);
  }

  public static <T> Sequence<T> withBaggage(final Sequence<T> seq, Closeable baggage)
  {
    return new ResourceClosingSequence<>(seq, baggage);
  }

  public static <T> Sequence<T> withEffect(final Sequence <T> seq, final Runnable effect, final Executor exec)
  {
    return new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
      {
        final OutType out = seq.accumulate(initValue, accumulator);
        exec.execute(effect);
        return out;
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        return new ExecuteWhenDoneYielder<>(seq.toYielder(initValue, accumulator), effect, exec);
      }
    };
  }

  // This will materialize the entire sequence in memory. Use at your own risk.
  public static <T> Sequence<T> sort(final Sequence<T> sequence, final Comparator<T> comparator)
  {
    List<T> seqList = Sequences.toList(sequence, Lists.<T>newArrayList());
    Collections.sort(seqList, comparator);
    return BaseSequence.simple(seqList);
  }

  public static <T, ListType extends List<T>> ListType toList(Sequence<T> seq, ListType list)
  {
    return seq.accumulate(list, Accumulators.<ListType, T>list());
  }

  private static class EmptySequence implements Sequence<Object>
  {
    @Override
    public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, Object> accumulator)
    {
      return initValue;
    }

    @Override
    public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, Object> accumulator)
    {
      return Yielders.done(initValue, null);
    }
  }
}
