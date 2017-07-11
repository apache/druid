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

package io.druid.query.topn;

import com.google.common.base.Function;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.topn.types.TopNColumnSelectorStrategy;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;

import java.util.Map;

/**
 * This has to be its own strategy because the pooled topn algorithm assumes each index is unique, and cannot handle multiple index numerals referencing the same dimension value.
 */
public class DimExtractionTopNAlgorithm extends BaseTopNAlgorithm<Aggregator[][], Map<Comparable, Aggregator[]>, TopNParams>
{
  private final TopNQuery query;

  public DimExtractionTopNAlgorithm(
      Capabilities capabilities,
      TopNQuery query
  )
  {
    super(capabilities);

    this.query = query;
  }

  @Override
  public TopNParams makeInitParams(
      final ColumnSelectorPlus<TopNColumnSelectorStrategy> selectorPlus,
      final Cursor cursor
  )
  {
    return new TopNParams(
        selectorPlus,
        cursor,
        Integer.MAX_VALUE
    );
  }

  @Override
  protected Aggregator[][] makeDimValSelector(TopNParams params, int numProcessed, int numToProcess)
  {
    if (params.getCardinality() < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with unknown cardinality");
    }
    ColumnSelectorPlus<TopNColumnSelectorStrategy> selectorPlus = params.getSelectorPlus();
    return selectorPlus.getColumnSelectorStrategy().getDimExtractionRowSelector(query, params, capabilities);
  }

  @Override
  protected Aggregator[][] updateDimValSelector(Aggregator[][] aggregators, int numProcessed, int numToProcess)
  {
    return aggregators;
  }

  @Override
  protected Map<Comparable, Aggregator[]> makeDimValAggregateStore(TopNParams params)
  {
    final ColumnSelectorPlus<TopNColumnSelectorStrategy> selectorPlus = params.getSelectorPlus();
    return selectorPlus.getColumnSelectorStrategy().makeDimExtractionAggregateStore();
  }

  @Override
  public long scanAndAggregate(
      TopNParams params,
      Aggregator[][] rowSelector,
      Map<Comparable, Aggregator[]> aggregatesStore,
      int numProcessed
  )
  {
    final Cursor cursor = params.getCursor();
    final ColumnSelectorPlus<TopNColumnSelectorStrategy> selectorPlus = params.getSelectorPlus();

    return selectorPlus.getColumnSelectorStrategy().dimExtractionScanAndAggregate(
        query,
        selectorPlus.getSelector(),
        cursor,
        rowSelector,
        aggregatesStore
    );
  }

  @Override
  protected void updateResults(
      TopNParams params,
      Aggregator[][] rowSelector,
      Map<Comparable, Aggregator[]> aggregatesStore,
      TopNResultBuilder resultBuilder
  )
  {
    final ColumnSelectorPlus<TopNColumnSelectorStrategy> selectorPlus = params.getSelectorPlus();
    final boolean needsResultTypeConversion = needsResultTypeConversion(params);
    final Function<Object, Object> valueTransformer = TopNMapFn.getValueTransformer(
        query.getDimensionSpec().getOutputType()
    );

    selectorPlus.getColumnSelectorStrategy().updateDimExtractionResults(
        aggregatesStore,
        needsResultTypeConversion ? valueTransformer : null,
        resultBuilder
    );
  }

  @Override
  protected void closeAggregators(Map<Comparable, Aggregator[]> valueMap)
  {
    for (Aggregator[] aggregators : valueMap.values()) {
      for (Aggregator agg : aggregators) {
        agg.close();
      }
    }
  }

  @Override
  public void cleanup(TopNParams params)
  {
  }

  private boolean needsResultTypeConversion(TopNParams params)
  {
    ColumnSelectorPlus<TopNColumnSelectorStrategy> selectorPlus = params.getSelectorPlus();
    TopNColumnSelectorStrategy strategy = selectorPlus.getColumnSelectorStrategy();
    return query.getDimensionSpec().getOutputType() != strategy.getValueType();
  }
}
