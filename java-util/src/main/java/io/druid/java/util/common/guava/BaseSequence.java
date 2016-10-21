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
import io.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 */
public class BaseSequence<T, IterType extends Iterator<T>> implements Sequence<T>
{
  private static final Logger log = new Logger(BaseSequence.class);

  private final IteratorMaker<T, IterType> maker;

  public static <T> Sequence<T> simple(final Iterable<T> iterable)
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return iterable.iterator();
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {

          }
        }
    );
  }

  public BaseSequence(
      IteratorMaker<T, IterType> maker
  )
  {
    this.maker = maker;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, final Accumulator<OutType, T> fn)
  {
    IterType iterator = maker.make();
    try {
      while (iterator.hasNext()) {
        initValue = fn.accumulate(initValue, iterator.next());
      }
      return initValue;
    }
    finally {
      maker.cleanup(iterator);
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final IterType iterator = maker.make();

    try {
      return makeYielder(initValue, accumulator, iterator);
    }
    catch (Exception e) {
      // We caught an Exception instead of returning a really, real, live, real boy, errr, iterator
      // So we better try to close our stuff, 'cause the exception is what is making it out of here.
      try {
        maker.cleanup(iterator);
      }
      catch (RuntimeException e1) {
        log.error(e1, "Exception thrown when closing maker.  Logging and ignoring.");
      }
      throw Throwables.propagate(e);
    }
  }

  private <OutType> Yielder<OutType> makeYielder(
      OutType initValue,
      final YieldingAccumulator<OutType, T> accumulator,
      final IterType iter
  )
  {
    OutType retVal = initValue;
    while (!accumulator.yielded() && iter.hasNext()) {
      retVal = accumulator.accumulate(retVal, iter.next());
    }

    if (!accumulator.yielded()) {
      return Yielders.done(
          retVal,
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              maker.cleanup(iter);
            }
          }
      );
    }

    final OutType finalRetVal = retVal;
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return finalRetVal;
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        accumulator.reset();
        try {
          return makeYielder(initValue, accumulator, iter);
        }
        catch (Exception e) {
          // We caught an Exception instead of returning a really, real, live, real boy, errr, iterator
          // So we better try to close our stuff, 'cause the exception is what is making it out of here.
          try {
            maker.cleanup(iter);
          }
          catch (RuntimeException e1) {
            log.error(e1, "Exception thrown when closing maker.  Logging and ignoring.");
          }
          throw Throwables.propagate(e);
        }
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close() throws IOException
      {
        maker.cleanup(iter);
      }
    };
  }

  public static interface IteratorMaker<T, IterType extends Iterator<T>>
  {
    public IterType make();

    public void cleanup(IterType iterFromMake);
  }
}
