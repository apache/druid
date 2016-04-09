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

package io.druid.common.guava;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 */
public class Sequences extends com.metamx.common.guava.Sequences
{
  public static <T> Iterable<T> toIterable(final Sequence<T> sequence)
  {
    return new Iterable<T>()
    {
      @Override
      public Iterator<T> iterator()
      {
        return toIterator(sequence);
      }
    };
  }

  public static <T> Iterator<T> toIterator(final Sequence<T> sequence)
  {
    final Yielder<T> start = sequence.toYielder(
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

    return new Iterator<T>()
    {
      private Yielder<T> yielder = start;

      @Override
      public boolean hasNext()
      {
        return !yielder.isDone();
      }

      @Override
      public T next()
      {
        T r = yielder.get();
        yielder = yielder.next(null);
        return r;
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public static <T> Sequence<T> sort(final Sequence<T> sequence, final Comparator<T> comparator, final int skip)
  {
    List<T> seqList = Sequences.toList(sequence, Lists.<T>newArrayList());
    Collections.sort(seqList, comparator);
    if (skip > 0) {
      return BaseSequence.simple(Iterables.skip(seqList, skip));
    }
    return BaseSequence.simple(seqList);
  }

  public static <T> Predicate<T> skipper(final int skip)
  {
    return new Predicate<T>()
    {
      private int counter = skip;

      @Override
      public boolean apply(T input)
      {
        return counter < 0 || --counter < 0;
      }
    };
  }
}
