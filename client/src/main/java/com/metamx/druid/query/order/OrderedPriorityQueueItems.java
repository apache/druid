package com.metamx.druid.query.order;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.metamx.druid.input.Row;
import com.metamx.druid.input.Rows;

import java.util.Iterator;

/**
 * Utility class that supports iterating a priority queue in sorted order.
 */
class OrderedPriorityQueueItems<T> implements Iterable<T>
{
  private MinMaxPriorityQueue<T> rows;
  public OrderedPriorityQueueItems(MinMaxPriorityQueue<T> rows)
  {
    this.rows = rows;
  }

  @Override
  public Iterator<T> iterator()
  {

    return new Iterator<T>() {

      @Override
      public boolean hasNext()
      {
        return !rows.isEmpty();
      }

      @Override
      public T next()
      {
        return rows.poll();
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("Can't remove any item from an intermediary heap for orderBy/limit");
      }
    };
  }
}
