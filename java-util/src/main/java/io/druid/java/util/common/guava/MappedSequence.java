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

import java.util.function.Function;

/**
 */
public class MappedSequence<T, Out> implements Sequence<Out>
{
  private final Sequence<T> baseSequence;
  private final Function<? super T, ? extends Out> fn;

  public MappedSequence(
      Sequence<T> baseSequence,
      Function<? super T, ? extends Out> fn
  )
  {
    this.baseSequence = baseSequence;
    this.fn = fn;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, Out> accumulator)
  {
    return baseSequence.accumulate(initValue, new MappingAccumulator<>(fn, accumulator));
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, Out> accumulator)
  {
    return baseSequence.toYielder(initValue, new MappingYieldingAccumulator<>(fn, accumulator));
  }
}
