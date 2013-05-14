package com.metamx.druid.query.group.orderby;

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
