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

import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A Sequence represents an iterable sequence of elements. Unlike normal Iterators however, it doesn't expose
 * a way for you to extract values from it, instead you provide it with a worker (an Accumulator) and that defines
 * what happens with the data.
 * <p>
 * This inversion of control is in place to allow the Sequence to do resource management. It can enforce that close()
 * methods get called and other resources get cleaned up whenever processing is complete. Without this inversion
 * it is very easy to unintentionally leak resources when iterating over something that is backed by a resource.
 * <p>
 * Sequences also expose {#see org.apache.druid.java.util.common.guava.Yielder} Yielder objects which allow you to implement a
 * continuation over the Sequence. Yielder do not offer the same guarantees of automatic resource management
 * as the accumulate method, but they are Closeable and will do the proper cleanup when close() is called on them.
 */
public interface Sequence<T>
{
  /**
   * Accumulate this sequence using the given accumulator.
   *
   * @param initValue   the initial value to pass along to start the accumulation.
   * @param accumulator the accumulator which is responsible for accumulating input values.
   * @param <OutType>   the type of accumulated value.
   *
   * @return accumulated value.
   */
  <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator);

  /**
   * Return a Yielder for accumulated sequence.
   *
   * @param initValue   the initial value to pass along to start the accumulation.
   * @param accumulator the accumulator which is responsible for accumulating input values.
   * @param <OutType>   the type of accumulated value.
   *
   * @return a Yielder for accumulated sequence.
   *
   * @see Yielder
   */
  <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator);

  default <U> Sequence<U> map(Function<? super T, ? extends U> mapper)
  {
    return new MappedSequence<>(this, mapper);
  }

  default Sequence<T> filter(Predicate<? super T> predicate)
  {
    return Sequences.filter(this, predicate);
  }

  /**
   * This will materialize the entire sequence.  Use at your own risk.
   */
  default List<T> toList()
  {
    return accumulate(new ArrayList<>(), Accumulators.list());
  }

  default Sequence<T> limit(long limit)
  {
    return new LimitedSequence<>(this, limit);
  }

  default <R> Sequence<R> flatMap(Function<? super T, ? extends Sequence<? extends R>> mapper)
  {
    return new ConcatSequence<>(this.map(mapper));
  }

  default <R> Sequence<R> flatMerge(
      Function<? super T, ? extends Sequence<? extends R>> mapper,
      Ordering<? super R> ordering
  )
  {
    return new MergeSequence<>(ordering, this.map(mapper));
  }

  default Sequence<T> withEffect(Runnable effect, Executor effectExecutor)
  {
    return Sequences.withEffect(this, effect, effectExecutor);
  }

  default Sequence<T> withBaggage(Closeable baggage)
  {
    return Sequences.withBaggage(this, baggage);
  }
}
