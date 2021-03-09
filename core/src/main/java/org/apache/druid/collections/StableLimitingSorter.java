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

import org.apache.druid.com.google.common.annotations.VisibleForTesting;
import org.apache.druid.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.druid.com.google.common.collect.Ordering;
import org.apache.druid.com.google.common.primitives.Ints;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

/**
 * Simultaneously sorts and limits its input.
 *
 * The sort is stable, meaning that equal elements (as determined by the comparator) will not be reordered.
 *
 * Not thread-safe.
 *
 * Note: this class doesn't have its own unit tests. It is tested along with
 * {@link org.apache.druid.java.util.common.guava.TopNSequence} in "TopNSequenceTest".
 */
public class StableLimitingSorter<T>
{
  private final MinMaxPriorityQueue<NumberedElement<T>> queue;
  private final int limit;

  private long size = 0;

  public StableLimitingSorter(final Comparator<T> comparator, final int limit)
  {
    this.limit = limit;
    this.queue = MinMaxPriorityQueue
        .orderedBy(
            Ordering.from(
                Comparator.<NumberedElement<T>, T>comparing(NumberedElement::getElement, comparator)
                    .thenComparing(NumberedElement::getNumber)
            )
        )
        .maximumSize(limit)
        .create();
  }

  /**
   * Offer an element to the sorter.
   */
  public void add(T element)
  {
    queue.offer(new NumberedElement<>(element, size++));
  }

  /**
   * Returns the number of elements currently in the sorter.
   */
  public int size()
  {
    return Ints.checkedCast(Math.min(size, limit));
  }

  /**
   * Drain elements in sorted order (least first).
   */
  public Iterator<T> drain()
  {
    return new Iterator<T>()
    {
      @Override
      public boolean hasNext()
      {
        return !queue.isEmpty();
      }

      @Override
      public T next()
      {
        return queue.poll().getElement();
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @VisibleForTesting
  static class NumberedElement<T>
  {
    private final T element;
    private final long number;

    public NumberedElement(T element, long number)
    {
      this.element = element;
      this.number = number;
    }

    public T getElement()
    {
      return element;
    }

    public long getNumber()
    {
      return number;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NumberedElement<?> that = (NumberedElement<?>) o;
      return number == that.number &&
             Objects.equals(element, that.element);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(element, number);
    }
  }
}
