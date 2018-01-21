/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;


import io.druid.common.config.NullHandling;
import io.druid.java.util.common.Pair;
import io.druid.segment.BaseNullableColumnValueSelector;
import io.druid.segment.ColumnSelectorFactory;

/**
 * abstract class with functionality to wrap aggregator/bufferAggregator/combiner to make them Nullable.
 * Implementations of {@link AggregatorFactory} which needs to Support Nullable Aggregations are encouraged
 * to extend this class.
 */
public abstract class NullableAggregatorFactory extends AggregatorFactory
{
  @Override
  public final Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    Pair<Aggregator, BaseNullableColumnValueSelector> pair = factorize2(
        metricFactory);
    return NullHandling.useDefaultValuesForNull() ? pair.lhs : new NullableAggregator(pair.lhs, pair.rhs);
  }

  protected abstract Pair<Aggregator, BaseNullableColumnValueSelector> factorize2(ColumnSelectorFactory metricfactory);

  @Override
  public final BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    Pair<BufferAggregator, BaseNullableColumnValueSelector> pair = factorizeBuffered2(
        metricFactory);
    return NullHandling.useDefaultValuesForNull() ? pair.lhs : new NullableBufferAggregator(pair.lhs, pair.rhs);
  }

  protected abstract Pair<BufferAggregator, BaseNullableColumnValueSelector> factorizeBuffered2(ColumnSelectorFactory metricfactory);


  @Override
  public final AggregateCombiner makeAggregateCombiner()
  {
    AggregateCombiner combiner = makeAggregateCombiner2();
    return NullHandling.useDefaultValuesForNull() ? combiner : new NullableAggregateCombiner(combiner);
  }

  protected abstract AggregateCombiner makeAggregateCombiner2();

  @Override
  public final int getMaxIntermediateSize()
  {
    return getMaxIntermediateSize2() + (NullHandling.useDefaultValuesForNull() ? 0 : Byte.BYTES);
  }

  protected abstract int getMaxIntermediateSize2();
}
