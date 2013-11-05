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

import com.metamx.druid.processing.ComplexMetricSelector;

public class TestAdaptiveCountingComplexMetricSelector<T> implements ComplexMetricSelector<T>
{
  private final T[] metrics;
  private final Class<T> clazz;

  private int index = 0;

  public TestAdaptiveCountingComplexMetricSelector(Class<T> clazz, T[] metrics)
  {
    this.clazz = clazz;
    this.metrics = metrics;
  }

  @Override
  public Class<T> classOfObject()
  {
    return clazz;
  }

  @Override
  public T get()
  {
    return metrics[index];
  }

  public void increment()
  {
    ++index;
  }
}
