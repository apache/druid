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

package org.apache.druid.query.topn.types;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.topn.BaseTopNAlgorithm;
import org.apache.druid.query.topn.TopNParams;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNResultBuilder;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class StringTopNColumnSelectorStrategy
    implements TopNColumnSelectorStrategy<DimensionSelector, Map<Comparable<?>, Aggregator[]>>
{
  private final Function<Object, Comparable<?>> dimensionValueConverter;

  public StringTopNColumnSelectorStrategy(final ValueType dimensionType)
  {
    this.dimensionValueConverter = DimensionHandlerUtils.converterFromTypeToType(ValueType.STRING, dimensionType);
  }

  @Override
  public int getCardinality(DimensionSelector selector)
  {
    return selector.getValueCardinality();
  }

  @Override
  public Aggregator[][] getDimExtractionRowSelector(TopNQuery query, TopNParams params, StorageAdapter storageAdapter)
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
        storageAdapter
    );

    return provider.build();
  }

  @Override
  public Map<Comparable<?>, Aggregator[]> makeDimExtractionAggregateStore()
  {
    return new HashMap<>();
  }

  @Override
  public long dimExtractionScanAndAggregate(
      TopNQuery query,
      DimensionSelector selector,
      Cursor cursor,
      Aggregator[][] rowSelector,
      Map<Comparable<?>, Aggregator[]> aggregatesStore
  )
  {
    if (selector.getValueCardinality() != DimensionDictionarySelector.CARDINALITY_UNKNOWN) {
      return dimExtractionScanAndAggregateWithCardinalityKnown(query, cursor, selector, rowSelector, aggregatesStore);
    } else {
      return dimExtractionScanAndAggregateWithCardinalityUnknown(query, cursor, selector, aggregatesStore);
    }
  }

  @Override
  public void updateDimExtractionResults(
      final Map<Comparable<?>, Aggregator[]> aggregatesStore,
      final TopNResultBuilder resultBuilder
  )
  {
    for (Map.Entry<Comparable<?>, Aggregator[]> entry : aggregatesStore.entrySet()) {
      Aggregator[] aggs = entry.getValue();
      if (aggs != null) {
        Object[] vals = new Object[aggs.length];
        for (int i = 0; i < aggs.length; i++) {
          vals[i] = aggs[i].get();
        }

        final Comparable<?> key = dimensionValueConverter.apply(entry.getKey());
        resultBuilder.addEntry(key, key, vals);
      }
    }
  }

  private long dimExtractionScanAndAggregateWithCardinalityKnown(
      TopNQuery query,
      Cursor cursor,
      DimensionSelector selector,
      Aggregator[][] rowSelector,
      Map<Comparable<?>, Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      final IndexedInts dimValues = selector.getRow();
      for (int i = 0, size = dimValues.size(); i < size; ++i) {
        final int dimIndex = dimValues.get(i);
        Aggregator[] theAggregators = rowSelector[dimIndex];
        if (theAggregators == null) {
          final Comparable<?> key = dimensionValueConverter.apply(selector.lookupName(dimIndex));
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
      Map<Comparable<?>, Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      final IndexedInts dimValues = selector.getRow();
      for (int i = 0, size = dimValues.size(); i < size; ++i) {
        final int dimIndex = dimValues.get(i);
        final Comparable<?> key = dimensionValueConverter.apply(selector.lookupName(dimIndex));

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
