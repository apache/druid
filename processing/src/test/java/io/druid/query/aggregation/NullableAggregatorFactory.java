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

package io.druid.query.aggregation;


import io.druid.common.config.NullHandling;
import io.druid.guice.annotations.ExtensionPoint;
import io.druid.segment.BaseNullableColumnValueSelector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;

/**
 * Abstract class with functionality to wrap {@link Aggregator}, {@link BufferAggregator} and {@link AggregateCombiner}
 * to support nullable aggregations for SQL compatibility. Implementations of {@link AggregatorFactory} which need to
 * Support Nullable Aggregations are encouraged to extend this class.
 */
@ExtensionPoint
public abstract class NullableAggregatorFactory<T extends BaseNullableColumnValueSelector> extends AggregatorFactory
{
  @Override
  public final Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    T selector = selector(metricFactory);
    Aggregator aggregator = factorize(metricFactory, selector);
    return NullHandling.replaceWithDefault() ? aggregator : new NullableAggregator(aggregator, selector);
  }

  @Override
  public final BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    T selector = selector(metricFactory);
    BufferAggregator aggregator = factorizeBuffered(metricFactory, selector);
    return NullHandling.replaceWithDefault() ? aggregator : new NullableBufferAggregator(aggregator, selector);
  }

  @Override
  public final AggregateCombiner makeNullableAggregateCombiner()
  {
    AggregateCombiner combiner = makeAggregateCombiner();
    return NullHandling.replaceWithDefault() ? combiner : new NullableAggregateCombiner(combiner);
  }

  @Override
  public final int getMaxIntermediateSizeWithNulls()
  {
    return getMaxIntermediateSize() + (NullHandling.replaceWithDefault() ? 0 : Byte.BYTES);
  }

  // ---- ABSTRACT METHODS BELOW ------

  /**
   * Creates a {@link ColumnValueSelector} for the aggregated column.
   *
   * @see ColumnValueSelector
   */
  protected abstract T selector(ColumnSelectorFactory metricFactory);

  /**
   * Creates an {@link Aggregator} to aggregate values from several rows, by using the provided selector.
   * @param metricFactory metricFactory
   * @param selector {@link ColumnValueSelector} for the column to aggregate.
   *
   * @see Aggregator
   */
  protected abstract Aggregator factorize(ColumnSelectorFactory metricFactory, T selector);

  /**
   * Creates an {@link BufferAggregator} to aggregate values from several rows into a ByteBuffer.
   * @param metricFactory metricFactory
   * @param selector {@link ColumnValueSelector} for the column to aggregate.
   *
   * @see BufferAggregator
   */
  protected abstract BufferAggregator factorizeBuffered(
      ColumnSelectorFactory metricFactory,
      T selector
  );
}
