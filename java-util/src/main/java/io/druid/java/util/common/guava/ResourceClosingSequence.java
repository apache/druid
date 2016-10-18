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

import java.io.Closeable;

/**
 */
public class ResourceClosingSequence<T> implements Sequence<T>
{
  private final Sequence<T> baseSequence;
  private final Closeable closeable;

  public ResourceClosingSequence(Sequence<T> baseSequence, Closeable closeable)
  {
    this.baseSequence = baseSequence;
    this.closeable = closeable;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    try {
      return baseSequence.accumulate(initValue, accumulator);
    }
    finally {
      CloseQuietly.close(closeable);
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue, YieldingAccumulator<OutType, T> accumulator
  )
  {
    final Yielder<OutType> baseYielder;
    try {
      baseYielder = baseSequence.toYielder(initValue, accumulator);
    }
    catch (RuntimeException e) {
      CloseQuietly.close(closeable);
      throw e;
    }

    return new ResourceClosingYielder<>(baseYielder, closeable);
  }
}
