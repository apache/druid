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

package io.druid.query.topn.types;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.topn.BaseTopNAlgorithm;
import io.druid.query.topn.TopNParams;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultBuilder;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;

import java.util.Map;

public class StringTopNColumnSelectorStrategy
    implements TopNColumnSelectorStrategy<DimensionSelector, Map<String, Aggregator[]>>
{
  @Override
  public int getCardinality(DimensionSelector selector)
  {
    return selector.getValueCardinality();
  }

  @Override
  public ValueType getValueType()
  {
    return ValueType.STRING;
  }

  @Override
  public Aggregator[][] getDimExtractionRowSelector(TopNQuery query, TopNParams params, Capabilities capabilities)
  {
    if (params.getCardinality() < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with unknown cardinality");
    }

    // This method is used for the DimExtractionTopNAlgorithm only.
    // Unlike regular topN we cannot rely on ordering to optimize.
    // Optimization possibly requires a reverse lookup from value to ID, which is
    // not possible when applying an extraction function
    final BaseTopNAlgorithm.AggregatorArrayProvider provider = new BaseTopNAlgorithm.AggregatorArrayProvider(
        (DimensionSelector) params.getSelectorPlus().getSelector(),
        query,
        params.getCardinality(),
        capabilities
    );

    return provider.build();
  }

  @Override
  public Map<String, Aggregator[]> makeDimExtractionAggregateStore()
  {
    return Maps.newHashMap();
  }

  @Override
  public long dimExtractionScanAndAggregate(
      TopNQuery query,
      DimensionSelector selector,
      Cursor cursor,
      Aggregator[][] rowSelector,
      Map<String, Aggregator[]> aggregatesStore
  )
  {
    if (selector.getValueCardinality() != DimensionSelector.CARDINALITY_UNKNOWN) {
      return dimExtractionScanAndAggregateWithCardinalityKnown(query, cursor, selector, rowSelector, aggregatesStore);
    } else {
      return dimExtractionScanAndAggregateWithCardinalityUnknown(query, cursor, selector, aggregatesStore);
    }
  }

  @Override
  public void updateDimExtractionResults(
      final Map<String, Aggregator[]> aggregatesStore,
      final Function<Object, Object> valueTransformer,
      final TopNResultBuilder resultBuilder
  )
  {
    for (Map.Entry<String, Aggregator[]> entry : aggregatesStore.entrySet()) {
      Aggregator[] aggs = entry.getValue();
      if (aggs != null) {
        Object[] vals = new Object[aggs.length];
        for (int i = 0; i < aggs.length; i++) {
          vals[i] = aggs[i].get();
        }

        Comparable key = entry.getKey();
        if (valueTransformer != null) {
          key = (Comparable) valueTransformer.apply(key);
        }

        resultBuilder.addEntry(
            key,
            key,
            vals
        );
      }
    }
  }

  private long dimExtractionScanAndAggregateWithCardinalityKnown(
      TopNQuery query,
      Cursor cursor,
      DimensionSelector selector,
      Aggregator[][] rowSelector,
      Map<String, Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      final IndexedInts dimValues = selector.getRow();
      for (int i = 0; i < dimValues.size(); ++i) {
        final int dimIndex = dimValues.get(i);
        Aggregator[] theAggregators = rowSelector[dimIndex];
        if (theAggregators == null) {
          final String key = selector.lookupName(dimIndex);
          theAggregators = aggregatesStore.get(key);
          if (theAggregators == null) {
            theAggregators = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
            aggregatesStore.put(key, theAggregators);
          }
          rowSelector[dimIndex] = theAggregators;
        }

        for (Aggregator aggregator : theAggregators) {
          aggregator.aggregate();
        }
      }
      cursor.advance();
      processedRows++;
    }
    return processedRows;
  }

  private long dimExtractionScanAndAggregateWithCardinalityUnknown(
      TopNQuery query,
      Cursor cursor,
      DimensionSelector selector,
      Map<String, Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      final IndexedInts dimValues = selector.getRow();
      for (int i = 0; i < dimValues.size(); ++i) {
        final int dimIndex = dimValues.get(i);
        final String key = selector.lookupName(dimIndex);

        Aggregator[] theAggregators = aggregatesStore.get(key);
        if (theAggregators == null) {
          theAggregators = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
          aggregatesStore.put(key, theAggregators);
        }
        for (Aggregator aggregator : theAggregators) {
          aggregator.aggregate();
        }
      }
      cursor.advance();
      processedRows++;
    }
    return processedRows;
  }
}
