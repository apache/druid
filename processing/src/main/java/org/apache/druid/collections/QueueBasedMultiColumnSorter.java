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

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.ISE;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

public class QueueBasedMultiColumnSorter<T> implements MultiColumnSorter<T>
{

  private final MinMaxPriorityQueue<MultiColumnSorter.MultiColumnSorterElement<T>> queue;

  public QueueBasedMultiColumnSorter(int limit, Comparator<MultiColumnSorter.MultiColumnSorterElement<T>> comparator)
  {
    this.queue = MinMaxPriorityQueue
        .orderedBy(Ordering.from(comparator))
        .maximumSize(limit)
        .create();
  }

  public QueueBasedMultiColumnSorter(int limit, Ordering<MultiColumnSorter.MultiColumnSorterElement<T>> ordering)
  {
    this.queue = MinMaxPriorityQueue
        .orderedBy(ordering)
        .maximumSize(limit)
        .create();
  }

  @Override
  public void add(MultiColumnSorter.MultiColumnSorterElement<T> sorterElement)
  {
    for (Comparable orderByColumn : sorterElement.getOrderByColumValues()) {
      if (Objects.isNull(orderByColumn)) {
        return;
      }
    }
    try {
      queue.offer(sorterElement);
    }
    catch (ClassCastException e) {
      throw new ISE("Multiple values of different types scanOrderBy are not allowed in the same column.");
    }
  }

  @Override
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
}
