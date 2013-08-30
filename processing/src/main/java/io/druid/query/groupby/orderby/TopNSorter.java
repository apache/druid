/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
