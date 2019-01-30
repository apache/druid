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

import java.util.Comparator;
import java.util.List;

/**
 * A wrapper around averagers that makes them appear to be aggregators.
 * This is necessary purely to allow existing common druid code that only knows
 * about aggregators to work with the MovingAverageQuery query as well.
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
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return null;
  }

  /**
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return null;
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
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return null;
  }

  /**
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return null;
  }

  /**
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return null;
  }

  /**
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public Object deserialize(Object object)
  {
    return null;
  }

  /**
   * Returns null because Averagers aren't actually Aggregators
   */
  @SuppressWarnings("unchecked")
  @Override
  public Object finalizeComputation(Object object)
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
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public List<String> requiredFields()
  {
    return null;
  }

  /**
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public byte[] getCacheKey()
  {
    return null;
  }

  /**
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public String getTypeName()
  {
    return null;
  }

  /**
   * Returns null because Averagers aren't actually Aggregators
   */
  @Override
  public int getMaxIntermediateSize()
  {
    return 0;
  }

}
