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

/**
*/
public class MappingYieldingAccumulator<OutType, InType, MappedType> extends YieldingAccumulator<OutType, InType>
{
  private final Function<InType, MappedType> fn;
  private final YieldingAccumulator<OutType, MappedType> baseAccumulator;

  public MappingYieldingAccumulator(
      Function<InType, MappedType> fn,
      YieldingAccumulator<OutType, MappedType> baseAccumulator
  ) {
    this.fn = fn;
    this.baseAccumulator = baseAccumulator;
  }

  @Override
  public void yield()
  {
    baseAccumulator.yield();
  }

  @Override
  public boolean yielded()
  {
    return baseAccumulator.yielded();
  }

  @Override
  public void reset()
  {
    baseAccumulator.reset();
  }

  @Override
  public OutType accumulate(OutType accumulated, InType in)
  {
    return baseAccumulator.accumulate(accumulated, fn.apply(in));
  }
}
