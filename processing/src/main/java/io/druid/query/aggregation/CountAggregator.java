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

package io.druid.query.aggregation;

import java.util.Comparator;

/**
 */
public class CountAggregator implements Aggregator
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

  static Object combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).longValue() + ((Number) rhs).longValue();
  }

  long count = 0;
  private final String name;

  public CountAggregator(String name)
  {
    this.name = name;
  }

  @Override
  public void aggregate()
  {
    ++count;
  }

  @Override
  public void reset()
  {
    count = 0;
  }

  @Override
  public Object get()
  {
    return count;
  }

  @Override
  public float getFloat()
  {
    return (float) count;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new CountAggregator(name);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
