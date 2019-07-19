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

package org.apache.druid.query.aggregation;


import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

/**
 * Abstract class with functionality to wrap {@link Aggregator}, {@link BufferAggregator} and {@link AggregateCombiner}
 * to support nullable aggregations for SQL compatibility. Implementations of {@link AggregatorFactory} which need to
 * Support Nullable Aggregations are encouraged to extend this class.
 */
@ExtensionPoint
public abstract class NullableAggregatorFactory<T extends BaseNullableColumnValueSelector> extends AggregatorFactory
{
  @Override
  public final Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    T selector = selector(columnSelectorFactory);
    Aggregator aggregator = factorize(columnSelectorFactory, selector);
    return NullHandling.replaceWithDefault() ? aggregator : new NullableAggregator(aggregator, selector);
  }

  @Override
  public final BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    T selector = selector(columnSelectorFactory);
    BufferAggregator aggregator = factorizeBuffered(columnSelectorFactory, selector);
    return NullHandling.replaceWithDefault() ? aggregator : new NullableBufferAggregator(aggregator, selector);
  }

  @Override
  public final VectorAggregator factorizeVector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    Preconditions.checkState(canVectorize(), "Cannot vectorize");
    VectorValueSelector selector = vectorSelector(columnSelectorFactory);
    VectorAggregator aggregator = factorizeVector(columnSelectorFactory, selector);
    return NullHandling.replaceWithDefault() ? aggregator : new NullableVectorAggregator(aggregator, selector);
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
  protected abstract T selector(ColumnSelectorFactory columnSelectorFactory);

  /**
   * Creates a {@link VectorValueSelector} for the aggregated column.
   *
   * @see VectorValueSelector
   */
  protected VectorValueSelector vectorSelector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    throw new UnsupportedOperationException("Cannot vectorize");
  }

  /**
   * Creates an {@link Aggregator} to aggregate values from several rows, by using the provided selector.
   *
   * @param columnSelectorFactory metricFactory
   * @param selector              {@link ColumnValueSelector} for the column to aggregate.
   *
   * @see Aggregator
   */
  protected abstract Aggregator factorize(ColumnSelectorFactory columnSelectorFactory, T selector);

  /**
   * Creates an {@link BufferAggregator} to aggregate values from several rows into a ByteBuffer.
   *
   * @param columnSelectorFactory columnSelectorFactory in case any other columns are needed.
   * @param selector              {@link ColumnValueSelector} for the column to aggregate.
   *
   * @see BufferAggregator
   */
  protected abstract BufferAggregator factorizeBuffered(
      ColumnSelectorFactory columnSelectorFactory,
      T selector
  );

  /**
   * Creates a {@link VectorAggregator} to aggregate values from several rows into a ByteBuffer.
   *
   * @param columnSelectorFactory columnSelectorFactory in case any other columns are needed.
   * @param selector              {@link VectorValueSelector} for the column to aggregate.
   *
   * @see BufferAggregator
   */
  protected VectorAggregator factorizeVector(
      // Not used by current aggregators, but here for parity with "factorizeBuffered".
      @SuppressWarnings("unused") VectorColumnSelectorFactory columnSelectorFactory,
      VectorValueSelector selector
  )
  {
    if (!canVectorize()) {
      throw new UnsupportedOperationException("Cannot vectorize");
    } else {
      throw new UnsupportedOperationException("canVectorize returned true but 'factorizeVector' is not implemented");
    }
  }
}
