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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.ISE;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class MultiColumnSorter<T>
{

  private final MinMaxPriorityQueue<MultiColumnSorterElement<T>> queue;

  public MultiColumnSorter(int limit, Comparator<MultiColumnSorterElement<T>> comparator)
  {
    this.queue = MinMaxPriorityQueue
        .orderedBy(Ordering.from(comparator))
        .maximumSize(limit)
        .create();
  }

  /**
   * Offer an element to the sorter.If there are multiple values of different types in the same column, a CalssCastException will be thrown
   */
  public void add(T element, List<Comparable> orderByColumns)
  {
    for (Comparable orderByColumn : orderByColumns) {
      if (Objects.isNull(orderByColumn)) {
        return;
      }
    }
    try {
      queue.offer(new MultiColumnSorterElement<>(element, orderByColumns));
    }
    catch (ClassCastException e) {
      throw new ISE("Multiple values of different types scanOrderBy are not allowed in the same column.");
    }
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
  public static class MultiColumnSorterElement<T>
  {
    private final T element;
    private final List<Comparable> orderByColumValues;

    public MultiColumnSorterElement(T element, List<Comparable> orderByColums)
    {
      this.element = element;
      this.orderByColumValues = orderByColums;
    }

    public T getElement()
    {
      return element;
    }

    public List<Comparable> getOrderByColumValues()
    {
      return orderByColumValues;
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
      MultiColumnSorterElement<?> that = (MultiColumnSorterElement<?>) o;
      return orderByColumValues == that.orderByColumValues &&
             Objects.equals(element, that.element);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(element, orderByColumValues);
    }
  }
}
