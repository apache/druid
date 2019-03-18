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

import java.io.Closeable;
import java.io.IOException;

/**
 */
public class ConcatSequence<T> implements Sequence<T>
{
  private final Sequence<Sequence<T>> baseSequences;

  public ConcatSequence(
      Sequence<? extends Sequence<? extends T>> baseSequences
  )
  {
    this.baseSequences = (Sequence<Sequence<T>>) baseSequences;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, final Accumulator<OutType, T> accumulator)
  {
    return baseSequences.accumulate(initValue, (accumulated, in) -> in.accumulate(accumulated, accumulator));
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      final OutType initValue,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    Yielder<Sequence<T>> yielderYielder = baseSequences.toYielder(
        null,
        new YieldingAccumulator<Sequence<T>, Sequence<T>>()
        {
          @Override
          public Sequence<T> accumulate(Sequence<T> accumulated, Sequence<T> in)
          {
            yield();
            return in;
          }
        }
    );

    try {
      return makeYielder(yielderYielder, initValue, accumulator);
    }
    catch (Throwable t) {
      try {
        yielderYielder.close();
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      throw t;
    }
  }

  public <OutType> Yielder<OutType> makeYielder(
      Yielder<Sequence<T>> yielderYielder,
      OutType initValue,
      YieldingAccumulator<OutType, T> accumulator
  )
  {
    while (!yielderYielder.isDone()) {
      Yielder<OutType> yielder = yielderYielder.get().toYielder(initValue, accumulator);
      if (accumulator.yielded()) {
        return wrapYielder(yielder, yielderYielder, accumulator);
      }

      initValue = yielder.get();
      try {
        yielder.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      yielderYielder = yielderYielder.next(null);
    }

    return Yielders.done(initValue, yielderYielder);
  }

  private <OutType> Yielder<OutType> wrapYielder(
      final Yielder<OutType> yielder,
      final Yielder<Sequence<T>> yielderYielder,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    if (!accumulator.yielded()) {
      OutType nextInit = yielder.get();
      try {
        yielder.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      return makeYielder(yielderYielder.next(null), nextInit, accumulator);
    }

    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return yielder.get();
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        return wrapYielder(yielder.next(initValue), yielderYielder, accumulator);
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close() throws IOException
      {
        try (Closeable toClose = yielderYielder) {
          yielder.close();
        }
      }
    };
  }
}
