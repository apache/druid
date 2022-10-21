/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.movingaverage;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.movingaverage.averagers.AveragerFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * A wrapper around averagers that makes them appear to be aggregators.
 * This is necessary purely to allow existing common druid code that only knows
 * about aggregators to work with the MovingAverageQuery query as well.
 *
 * NOTE: The {@link AggregatorFactory} abstract class is only partially extended.
 * Most methods are not implemented and throw {@link UnsupportedOperationException} if called.
 * This is because these methods are invalid for the AveragerFactoryWrapper.
 *
 * @param <T> Result type
 * @param <R> Finalized Result type
 */
public class AveragerFactoryWrapper<T, R> extends AggregatorFactory
{
  private final AveragerFactory<T, R> af;
  private final String prefix;

  /**
   * Simple constructor
   *
   * @param af
   * @param prefix
   */
  public AveragerFactoryWrapper(AveragerFactory<T, R> af, String prefix)
  {
    this.af = af;
    this.prefix = prefix;
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory) throws UnsupportedOperationException
  {
    throw new UnsupportedOperationException("Invalid operation for AveragerFactoryWrapper.");
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException("Invalid operation for AveragerFactoryWrapper.");
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#getComparator()
   */
  @Override
  public Comparator<?> getComparator()
  {
    return af.getComparator();
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @Override
  public Object combine(Object lhs, Object rhs)
  {
    throw new UnsupportedOperationException("Invalid operation for AveragerFactoryWrapper.");
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @Override
  public AggregatorFactory getCombiningFactory()
  {
    throw new UnsupportedOperationException("Invalid operation for AveragerFactoryWrapper.");
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    throw new UnsupportedOperationException("Invalid operation for AveragerFactoryWrapper.");
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @Override
  public Object deserialize(Object object)
  {
    throw new UnsupportedOperationException("Invalid operation for AveragerFactoryWrapper.");
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return af.finalizeComputation((T) object);
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.AggregatorFactory#getName()
   */
  @Override
  public String getName()
  {
    return prefix + af.getName();
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @Override
  public List<String> requiredFields()
  {
    throw new UnsupportedOperationException("Invalid operation for AveragerFactoryWrapper.");
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @Override
  public byte[] getCacheKey()
  {
    throw new UnsupportedOperationException("Invalid operation for AveragerFactoryWrapper.");
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.UNKNOWN_COMPLEX;
  }

  @Override
  public ColumnType getResultType()
  {
    return getIntermediateType();
  }

  /**
   * Not implemented. Throws UnsupportedOperationException.
   */
  @Override
  public int getMaxIntermediateSize()
  {
    throw new UnsupportedOperationException("Invalid operation for AveragerFactoryWrapper.");
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new AveragerFactoryWrapper(af, newName);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AveragerFactoryWrapper<?, ?> that = (AveragerFactoryWrapper<?, ?>) o;
    return af.equals(that.af) && prefix.equals(that.prefix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(af, prefix);
  }
}
