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

import com.google.common.collect.MinMaxPriorityQueue;

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
