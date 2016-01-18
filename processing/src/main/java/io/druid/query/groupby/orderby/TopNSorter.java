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

package io.druid.query.groupby.orderby;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;

/**
 * A utility class that sorts a list of comparable items in the given order, and keeps only the
 * top N sorted items.
 */
public class TopNSorter<T>
{
  private Ordering<T> ordering;

  /**
   * Constructs a sorter that will sort items with given ordering.
   * @param ordering the order that this sorter instance will use for sorting
   */
  public TopNSorter(Ordering<T> ordering)
  {
    this.ordering = ordering;
  }

  /**
   * Sorts a list of rows and retain the top n items
   * @param items the collections of items to be sorted
   * @param n the number of items to be retained
   * @return Top n items that are sorted in the order specified when this instance is constructed.
   */
  public Iterable<T> toTopN(Iterable<T> items, int n)
  {
    if(n <= 0) {
      return ImmutableList.of();
    }

    MinMaxPriorityQueue<T> queue = MinMaxPriorityQueue.orderedBy(ordering).maximumSize(n).create(items);

    return new OrderedPriorityQueueItems<T>(queue);
  }
}
