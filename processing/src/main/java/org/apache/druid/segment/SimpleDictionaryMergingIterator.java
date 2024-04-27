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

package org.apache.druid.segment;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.data.Indexed;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Use a {@link PriorityQueue} to merge sorted {@link Indexed} based value-lookups using {@link PeekingIterator}
 */
public class SimpleDictionaryMergingIterator<T extends Comparable<T>> implements CloseableIterator<T>
{
  public static <T extends Comparable<T>> Comparator<PeekingIterator<T>> makePeekingComparator()
  {
    return (lhs, rhs) -> {
      T left = lhs.peek();
      T right = rhs.peek();
      if (left == null) {
        //noinspection VariableNotUsedInsideIf
        return right == null ? 0 : -1;
      } else if (right == null) {
        return 1;
      } else {
        return left.compareTo(right);
      }
    };
  }

  protected final PriorityQueue<PeekingIterator<T>> pQueue;
  protected int counter;

  public SimpleDictionaryMergingIterator(
      Indexed<T>[] dimValueLookups,
      Comparator<PeekingIterator<T>> comparator
  )
  {
    pQueue = new PriorityQueue<>(dimValueLookups.length, comparator);

    for (Indexed<T> dimValueLookup : dimValueLookups) {
      if (dimValueLookup == null) {
        continue;
      }
      final PeekingIterator<T> iter = Iterators.peekingIterator(dimValueLookup.iterator());
      if (iter.hasNext()) {
        pQueue.add(iter);
      }
    }
  }

  @Override
  public boolean hasNext()
  {
    return !pQueue.isEmpty();
  }

  @Override
  public T next()
  {
    PeekingIterator<T> smallest = pQueue.remove();
    if (smallest == null) {
      throw new NoSuchElementException();
    }
    final T value = smallest.next();
    if (smallest.hasNext()) {
      pQueue.add(smallest);
    }

    while (!pQueue.isEmpty() && Objects.equals(value, pQueue.peek().peek())) {
      PeekingIterator<T> same = pQueue.remove();
      same.next();
      if (same.hasNext()) {
        pQueue.add(same);
      }
    }
    counter++;

    return value;
  }

  public int getCardinality()
  {
    return counter;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException("remove");
  }

  @Override
  public void close()
  {
    // nothing to do
  }
}
