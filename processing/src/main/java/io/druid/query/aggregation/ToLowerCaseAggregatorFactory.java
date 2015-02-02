/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  public List<AggregatorFactory> getRequiredColumns()
  {
    return baseAggregatorFactory.getRequiredColumns();
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
