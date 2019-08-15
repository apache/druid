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

package org.apache.druid.collections;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BinaryOperator;

/**
 */
public class CombiningIterator<T> implements Iterator<T>
{
  public static <T> CombiningIterator<T> create(
      Iterator<T> it,
      Comparator<T> comparator,
      BinaryOperator<T> fn
  )
  {
    return new CombiningIterator<>(it, comparator, fn);
  }

  private final PeekingIterator<T> it;
  private final Comparator<T> comparator;
  private final BinaryOperator<T> fn;

  public CombiningIterator(
      Iterator<T> it,
      Comparator<T> comparator,
      BinaryOperator<T> fn
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
  public T next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    T res = null;

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
