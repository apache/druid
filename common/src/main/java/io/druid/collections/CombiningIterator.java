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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import io.druid.java.util.common.guava.nary.BinaryFn;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class CombiningIterator<InType> implements Iterator<InType>
{
  public static <InType> CombiningIterator<InType> create(
      Iterator<InType> it,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, InType> fn
  )
  {
    return new CombiningIterator<InType>(it, comparator, fn);
  }

  private final PeekingIterator<InType> it;
  private final Comparator<InType> comparator;
  private final BinaryFn<InType, InType, InType> fn;

  public CombiningIterator(
      Iterator<InType> it,
      Comparator<InType> comparator,
      BinaryFn<InType, InType, InType> fn
  )
  {
    this.it = Iterators.peekingIterator(it);
    this.comparator = comparator;
    this.fn = fn;
  }

  @Override
  public boolean hasNext()
  {
    return it.hasNext();
  }

  @Override
  public InType next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    InType res = null;

    while (hasNext()) {
      if (res == null) {
        res = fn.apply(it.next(), null);
        continue;
      }

      if (comparator.compare(res, it.peek()) == 0) {
        res = fn.apply(res, it.next());
      } else {
        break;
      }
    }

    return res;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
