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

import io.druid.segment.ColumnSelectorFactory;

import java.util.Comparator;
import java.util.List;

/**
 */
public class ToLowerCaseAggregatorFactory implements AggregatorFactory
{
  private final AggregatorFactory baseAggregatorFactory;

  public ToLowerCaseAggregatorFactory(AggregatorFactory baseAggregatorFactory)
  {
    this.baseAggregatorFactory = baseAggregatorFactory;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return baseAggregatorFactory.factorize(metricFactory);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return baseAggregatorFactory.factorizeBuffered(metricFactory);
  }

  @Override
  public Comparator getComparator()
  {
    return baseAggregatorFactory.getComparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return baseAggregatorFactory.combine(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return baseAggregatorFactory.getCombiningFactory();
  }

  @Override
  public Object deserialize(Object object)
  {
    return baseAggregatorFactory.deserialize(object);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return baseAggregatorFactory.finalizeComputation(object);
  }

  @Override
  public String getName()
  {
    return baseAggregatorFactory.getName().toLowerCase();
  }

  @Override
  public List<String> requiredFields()
  {
    return baseAggregatorFactory.requiredFields();
  }

  @Override
  public byte[] getCacheKey()
  {
    return baseAggregatorFactory.getCacheKey();
  }

  @Override
  public String getTypeName()
  {
    return baseAggregatorFactory.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return baseAggregatorFactory.getMaxIntermediateSize();
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return baseAggregatorFactory.getAggregatorStartValue();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ToLowerCaseAggregatorFactory that = (ToLowerCaseAggregatorFactory) o;

    if (baseAggregatorFactory != null ? !baseAggregatorFactory.equals(that.baseAggregatorFactory) : that.baseAggregatorFactory != null)
      return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    return baseAggregatorFactory != null ? baseAggregatorFactory.hashCode() : 0;
  }
}
