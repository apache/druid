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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

/**
 */
final class WrappingSequence<T> implements Sequence<T>
{
  private final Sequence<T> baseSequence;
  private final SequenceWrapper wrapper;

  WrappingSequence(Sequence<T> baseSequence, SequenceWrapper wrapper)
  {
    this.baseSequence = Preconditions.checkNotNull(baseSequence, "baseSequence");
    this.wrapper = Preconditions.checkNotNull(wrapper, "wrapper");
  }

  @Override
  public <OutType> OutType accumulate(final OutType outType, final Accumulator<OutType, T> accumulator)
  {
    OutType result;
    try {
      wrapper.before();
      result = wrapper.wrap(new Supplier<OutType>()
      {
        @Override
        public OutType get()
        {
          return baseSequence.accumulate(outType, accumulator);
        }
      });
    }
    catch (Throwable t) {
      // Close on failure
      try {
        wrapper.after(false, t);
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      Throwables.propagateIfPossible(t);
      throw new RuntimeException(t);
    }
    // "Normal" close
    try {
      wrapper.after(true, null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      final OutType initValue,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    try {
      wrapper.before();
      return wrapper.wrap(new Supplier<Yielder<OutType>>()
      {
        @Override
        public Yielder<OutType> get()
        {
          return new WrappingYielder<>(baseSequence.toYielder(initValue, accumulator), wrapper);
        }
      });
    }
    catch (Throwable t) {
      // Close on failure
      try {
        wrapper.after(false, t);
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      Throwables.propagateIfPossible(t);
      throw new RuntimeException(t);
    }
  }
}
