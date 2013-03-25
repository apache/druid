/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.aggregation;

import com.metamx.druid.processing.FloatMetricSelector;

import java.util.Comparator;

/**
 */
public class DoubleSumAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return ((Double) o).compareTo((Double) o1);
    }
  };

  static double combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
  }

  private final FloatMetricSelector selector;
  private final String name;

  private double sum;

  public DoubleSumAggregator(String name, FloatMetricSelector selector)
  {
    this.name = name;
    this.selector = selector;

    this.sum = 0;
  }

  @Override
  public void aggregate()
  {
    sum += selector.get();
  }

  @Override
  public void reset()
  {
    sum = 0;
  }

  @Override
  public Object get()
  {
    return sum;
  }

  @Override
  public float getFloat()
  {
    return (float) sum;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new DoubleSumAggregator(name, selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
