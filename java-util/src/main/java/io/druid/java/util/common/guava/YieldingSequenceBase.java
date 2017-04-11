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

/**
 * A Sequence that is based entirely on the Yielder implementation.
 * <p/>
 * This is a base class to simplify the creation of Sequences.
 */
public abstract class YieldingSequenceBase<T> implements Sequence<T>
{
  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    Yielder<OutType> yielder = toYielder(initValue, YieldingAccumulators.fromAccumulator(accumulator));

    try {
      return yielder.get();
    }
    finally {
      CloseQuietly.close(yielder);
    }
  }
}
