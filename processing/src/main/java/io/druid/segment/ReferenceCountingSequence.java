/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment;

import com.metamx.common.guava.ResourceClosingYielder;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingSequenceBase;

import java.io.Closeable;
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
    final Closeable closeable = segment.increment();
    final Yielder<OutType> yielder = baseSequence.toYielder(initValue, accumulator);
    try {
      return new ResourceClosingYielder<OutType>(baseSequence.toYielder(initValue, accumulator), closeable);
    }
    catch (RuntimeException ex) {
      try {
        yielder.close();
      }
      catch (IOException e) {
        ex.addSuppressed(e);
      }
      throw ex;
    }
  }
}
