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

import io.druid.query.filter.ValueMatcher;

public class FilteredAggregator implements Aggregator
{
  private final ValueMatcher matcher;
  private final Aggregator delegate;
  private final String name;

  public FilteredAggregator(String name, ValueMatcher matcher, Aggregator delegate)
  {
    this.matcher = matcher;
    this.delegate = delegate;
    this.name = name;
  }

  @Override
  public void aggregate()
  {
    if (matcher.matches()) {
      delegate.aggregate();
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
    return name;
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
