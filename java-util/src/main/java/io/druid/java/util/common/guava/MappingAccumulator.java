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
public class MappingAccumulator<OutType, InType, MappedType> implements Accumulator<OutType, InType>
{
  private final Function<InType, MappedType> fn;
  private final Accumulator<OutType, MappedType> accumulator;

  public MappingAccumulator(
      Function<InType, MappedType> fn,
      Accumulator<OutType, MappedType> accumulator
  ) {
    this.fn = fn;
    this.accumulator = accumulator;
  }

  @Override
  public OutType accumulate(OutType accumulated, InType in)
  {
    return accumulator.accumulate(accumulated, fn.apply(in));
  }
}
