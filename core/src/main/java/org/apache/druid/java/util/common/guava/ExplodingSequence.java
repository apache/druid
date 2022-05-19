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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps an underlying sequence and allows us to force it to explode at various points.
 */
public class ExplodingSequence<T> extends YieldingSequenceBase<T>
{
  private final Sequence<T> baseSequence;
  private final boolean getThrowsException;
  private final boolean closeThrowsException;
  private final AtomicLong closed = new AtomicLong();

  public ExplodingSequence(Sequence<T> baseSequence, boolean getThrowsException, boolean closeThrowsException)
  {
    this.baseSequence = baseSequence;
    this.getThrowsException = getThrowsException;
    this.closeThrowsException = closeThrowsException;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      final OutType initValue,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    return wrapYielder(baseSequence.toYielder(initValue, accumulator));
  }

  public long getCloseCount()
  {
    return closed.get();
  }

  private <OutType> Yielder<OutType> wrapYielder(final Yielder<OutType> baseYielder)
  {
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        if (getThrowsException) {
          throw new RuntimeException("get");
        } else {
          return baseYielder.get();
        }
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        return wrapYielder(baseYielder.next(initValue));
      }

      @Override
      public boolean isDone()
      {
        return baseYielder.isDone();
      }

      @Override
      public void close() throws IOException
      {
        closed.incrementAndGet();

        if (closeThrowsException) {
          throw new IOException("close");
        } else {
          baseYielder.close();
        }
      }
    };
  }
}
