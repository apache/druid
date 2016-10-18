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

import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class ExecutorExecutingSequence<T> implements Sequence<T>
{
  private final Sequence<T> sequence;
  private final ExecutorService exec;

  public ExecutorExecutingSequence(
      Sequence<T> sequence,
      ExecutorService exec
  )
  {
    this.sequence = sequence;
    this.exec = exec;
  }

  @Override
  public <OutType> OutType accumulate(final OutType initValue, final Accumulator<OutType, T> accumulator)
  {
    Future<OutType> future = exec.submit(
        new Callable<OutType>()
        {
          @Override
          public OutType call() throws Exception
          {
            return sequence.accumulate(initValue, accumulator);
          }
        }
    );
    try {
      return future.get();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(final OutType initValue, final YieldingAccumulator<OutType, T> accumulator)
  {
    Future<Yielder<OutType>> future = exec.submit(
        new Callable<Yielder<OutType>>()
        {
          @Override
          public Yielder<OutType> call() throws Exception
          {
            return makeYielder(sequence.toYielder(initValue, accumulator));
          }
        }
    );
    try {
      return future.get();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  private <OutType> Yielder<OutType> makeYielder(final Yielder<OutType> yielder)
  {
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return yielder.get();
      }

      @Override
      public Yielder<OutType> next(final OutType initValue)
      {
        Future<Yielder<OutType>> future = exec.submit(
            new Callable<Yielder<OutType>>()
            {
              @Override
              public Yielder<OutType> call() throws Exception
              {
                return makeYielder(yielder.next(initValue));
              }
            }
        );
        try {
          return future.get();
        }
        catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public boolean isDone()
      {
        return yielder.isDone();
      }

      @Override
      public void close() throws IOException
      {
        yielder.close();
      }
    };
  }
}
