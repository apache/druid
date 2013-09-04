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

import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

/**
 */
public class MinAggregator implements Aggregator
{
  static final Comparator COMPARATOR = DoubleSumAggregator.COMPARATOR;

  static double combineValues(Object lhs, Object rhs)
  {
    return Math.min(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
  }

  private final FloatColumnSelector selector;
  private final String name;

  private double min;

  public MinAggregator(String name, FloatColumnSelector selector)
  {
    this.name = name;
    this.selector = selector;

    reset();
  }

  @Override
  public void aggregate()
  {
    min = Math.min(min, (double) selector.get());
  }

  @Override
  public void reset()
  {
    min = Double.POSITIVE_INFINITY;
  }

  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public float getFloat()
  {
    return (float) min;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new MinAggregator(name, selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
