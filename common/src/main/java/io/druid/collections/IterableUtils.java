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

package io.druid.collections;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;

/**
 */
public class IterableUtils
{
  // simple cartesian iterable
  public static Iterable<Object[]> cartesian(final Iterable... iterables)
  {
    return cartesian(Object.class, iterables);
  }

  @SafeVarargs
  public static <T> Iterable<T[]> cartesian(final Class<T> clazz, final Iterable<T>... iterables)
  {
    return new Iterable<T[]>()
    {
      @Override
      public Iterator<T[]> iterator()
      {
        return new Iterator<T[]>()
        {
          private final Iterator<T>[] iterators = new Iterator[iterables.length];

          private final T[] cached = (T[]) Array.newInstance(clazz, iterables.length);
          private final BitSet valid = new BitSet(iterables.length);

          @Override
          public boolean hasNext()
          {
            return hasNext(0);
          }

          private boolean hasNext(int index)
          {
            if (iterators[index] == null) {
              iterators[index] = iterables[index].iterator();
            }
            for (; hasMore(index); valid.clear(index)) {
              if (index == iterables.length - 1 || hasNext(index + 1)) {
                return true;
              }
            }
            iterators[index] = null;
            return false;
          }

          private boolean hasMore(int index)
          {
            return valid.get(index) || iterators[index].hasNext();
          }

          @Override
          public T[] next()
          {
            for (int index = 0; index < iterables.length; index++) {
              if (!valid.get(index)) {
                cached[index] = iterators[index].next();
                valid.set(index);
              }
            }
            T[] result = Arrays.copyOf(cached, cached.length);
            valid.clear(cached.length - 1);
            return result;
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException("remove");
          }
        };
      }
    };
  }
}
