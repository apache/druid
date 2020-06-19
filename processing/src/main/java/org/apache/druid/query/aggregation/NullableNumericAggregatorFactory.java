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
 * Abstract superclass for null-aware numeric aggregators.
 *
 * Includes functionality to wrap {@link Aggregator}, {@link BufferAggregator}, {@link VectorAggregator}, and
 * {@link AggregateCombiner} to support nullable aggregations. The result of this aggregator will be null if all the
 * values to be aggregated are null values, or if no values are aggregated at all. If any of the values are non-null,
 * the result will be the aggregated value of the non-null values.
 *
 * This superclass should only be extended by aggregators that read primitive numbers. It implements logic that is
 * not valid for non-numeric selector methods such as {@link ColumnValueSelector#getObject()}.
 *
 * @see BaseNullableColumnValueSelector#isNull() for why this only works in the numeric case
 */
@ExtensionPoint
public abstract class NullableNumericAggregatorFactory<T extends BaseNullableColumnValueSelector>
    extends AggregatorFactory
{
  @Override
  public final Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    T selector = selector(columnSelectorFactory);
    Aggregator aggregator = factorize(columnSelectorFactory, selector);
    return NullHandling.replaceWithDefault() ? aggregator : new NullableNumericAggregator(aggregator, selector);
  }

  @Override
  public final BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    T selector = selector(columnSelectorFactory);
    BufferAggregator aggregator = factorizeBuffered(columnSelectorFactory, selector);
    return NullHandling.replaceWithDefault() ? aggregator : new NullableNumericBufferAggregator(aggregator, selector);
  }

  @Override
  public final VectorAggregator factorizeVector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    Preconditions.checkState(canVectorize(columnSelectorFactory), "Cannot vectorize");
    VectorValueSelector selector = vectorSelector(columnSelectorFactory);
    VectorAggregator aggregator = factorizeVector(columnSelectorFactory, selector);
    return NullHandling.replaceWithDefault() ? aggregator : new NullableNumericVectorAggregator(aggregator, selector);
  }

  @Override
  public final AggregateCombiner makeNullableAggregateCombiner()
  {
    AggregateCombiner combiner = makeAggregateCombiner();
    return NullHandling.replaceWithDefault() ? combiner : new NullableNumericAggregateCombiner(combiner);
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
      VectorColumnSelectorFactory columnSelectorFactory,
      VectorValueSelector selector
  )
  {
    if (!canVectorize(columnSelectorFactory)) {
      throw new UnsupportedOperationException("Cannot vectorize");
    } else {
      throw new UnsupportedOperationException("canVectorize returned true but 'factorizeVector' is not implemented");
    }
  }
}
