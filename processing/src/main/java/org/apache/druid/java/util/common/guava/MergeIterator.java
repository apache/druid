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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * Iterator that merges a collection of sorted iterators using a comparator.
 *
 * Similar to Guava's MergingIterator, but avoids calling next() on any iterator prior to returning the value
 * returned by the previous call to next(). This is important when merging iterators that reuse container objects
 * across calls to next().
 *
 * Used by {@link org.apache.druid.java.util.common.collect.Utils#mergeSorted(Iterable, Comparator)}.
 */
public class MergeIterator<T> implements Iterator<T>
{
  private static final int PRIORITY_QUEUE_INITIAL_CAPACITY = 16;

  private final PriorityQueue<PeekingIterator<T>> pQueue;
  private PeekingIterator<T> currentIterator = null;

  public MergeIterator(
      final Iterable<? extends Iterator<? extends T>> sortedIterators,
      final Comparator<? super T> comparator
  )
  {
    pQueue = new PriorityQueue<>(
        PRIORITY_QUEUE_INITIAL_CAPACITY,
        (lhs, rhs) -> comparator.compare(lhs.peek(), rhs.peek())
    );

    for (final Iterator<? extends T> iterator : sortedIterators) {
      final PeekingIterator<T> iter = Iterators.peekingIterator(iterator);

      if (iter != null && iter.hasNext()) {
        pQueue.add(iter);
      }
    }

  }

  @Override
  public boolean hasNext()
  {
    return !pQueue.isEmpty() || (currentIterator != null && currentIterator.hasNext());
  }

  @Override
  public T next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    if (currentIterator != null) {
      if (currentIterator.hasNext()) {
        pQueue.add(currentIterator);
      }

      currentIterator = null;
    }

    PeekingIterator<T> retIt = pQueue.remove();
    T retVal = retIt.next();

    // Save currentIterator and add it back later, to avoid calling next() prior to returning the current value.
    currentIterator = retIt;
    return retVal;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
