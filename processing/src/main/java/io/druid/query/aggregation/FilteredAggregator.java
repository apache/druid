/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.query.aggregation;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

public class FilteredAggregator implements Aggregator
{
  private final DimensionSelector dimSelector;
  private final Aggregator delegate;
  private final IntPredicate predicate;

  public FilteredAggregator(DimensionSelector dimSelector, IntPredicate predicate, Aggregator delegate)
  {
    this.dimSelector = dimSelector;
    this.delegate = delegate;
    this.predicate = predicate;
  }

  @Override
  public void aggregate()
  {
    final IndexedInts row = dimSelector.getRow();
    final int size = row.size();
    for (int i = 0; i < size; ++i) {
      if (predicate.apply(row.get(i))) {
        delegate.aggregate();
        break;
      }
    }
  }

  @Override
  public void reset()
  {
    delegate.reset();
  }

  @Override
  public Object get()
  {
    return delegate.get();
  }

  @Override
  public float getFloat()
  {
    return delegate.getFloat();
  }

  @Override
  public String getName()
  {
    return delegate.getName();
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
