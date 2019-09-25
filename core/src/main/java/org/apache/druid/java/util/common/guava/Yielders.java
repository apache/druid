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

import com.google.common.base.Throwables;

import java.io.IOException;

/**
 */
public class Yielders
{
  public static <T> Yielder<T> each(final Sequence<T> sequence)
  {
    return sequence.toYielder(
        null,
        new YieldingAccumulator<T, T>()
        {
          @Override
          public T accumulate(T accumulated, T in)
          {
            yield();
            return in;
          }
        }
    );
  }

  public static <T> Yielder<T> done(final T finalVal, final AutoCloseable closeable)
  {
    return new Yielder<T>()
    {
      @Override
      public T get()
      {
        return finalVal;
      }

      @Override
      public Yielder<T> next(T initValue)
      {
        return null;
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public void close() throws IOException
      {
        if (closeable != null) {
          try {
            closeable.close();
          }
          catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, IOException.class);
            throw new RuntimeException(e);
          }
        }
      }
    };
  }
}
