package com.metamx.druid.query.order;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.metamx.druid.input.Row;
import com.metamx.druid.input.Rows;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: dyuan
 * Date: 3/18/13
 * Time: 10:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class OrderedQueueItems<T> implements Iterable<T>
{
  private MinMaxPriorityQueue<T> rows;
  public OrderedQueueItems(MinMaxPriorityQueue<T> rows)
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
