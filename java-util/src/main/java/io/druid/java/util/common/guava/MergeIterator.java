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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
*/
public class MergeIterator<T> implements Iterator<T>
{
  private final PriorityQueue<PeekingIterator<T>> pQueue;

  public MergeIterator(
      final Comparator<T> comparator,
      List<Iterator<T>> iterators
  )
  {
    pQueue = new PriorityQueue<>(
        16,
        new Comparator<PeekingIterator<T>>()
        {
          @Override
          public int compare(PeekingIterator<T> lhs, PeekingIterator<T> rhs)
          {
            return comparator.compare(lhs.peek(), rhs.peek());
          }
        }
    );

    for (Iterator<T> iterator : iterators) {
      final PeekingIterator<T> iter = Iterators.peekingIterator(iterator);

      if (iter != null && iter.hasNext()) {
        pQueue.add(iter);
      }
    }

  }

  @Override
  public boolean hasNext()
  {
    return ! pQueue.isEmpty();
  }

  @Override
  public T next()
  {
    if (! hasNext()) {
      throw new NoSuchElementException();
    }

    PeekingIterator<T> retIt = pQueue.remove();
    T retVal = retIt.next();

    if (retIt.hasNext()) {
      pQueue.add(retIt);
    }

    return retVal;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
