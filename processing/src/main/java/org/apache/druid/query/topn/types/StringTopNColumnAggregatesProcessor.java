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
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class StringTopNColumnAggregatesProcessor implements TopNColumnAggregatesProcessor<DimensionSelector>
{
  private final ColumnCapabilities capabilities;
  private final Function<Object, Comparable<?>> dimensionValueConverter;
  private HashMap<Comparable<?>, Aggregator[]> aggregatesStore;

  public StringTopNColumnAggregatesProcessor(final ColumnCapabilities capabilities, final ValueType dimensionType)
  {
    this.capabilities = capabilities;
    this.dimensionValueConverter = DimensionHandlerUtils.converterFromTypeToType(ValueType.STRING, dimensionType);
  }

  @Override
  public int getCardinality(DimensionSelector selector)
  {
    // only report the underlying selector cardinality if the column the selector is for is dictionary encoded, and
    // the dictionary values are unique, that is they have a 1:1 mapping between dictionaryId and column value
    if (capabilities.isDictionaryEncoded().and(capabilities.areDictionaryValuesUnique()).isTrue()) {
      return selector.getValueCardinality();
    }
    return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public Aggregator[][] getRowSelector(TopNQuery query, TopNParams params, StorageAdapter storageAdapter)
  {
    if (params.getCardinality() < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with unknown cardinality");
    }

    // This method is used for the HeapBasedTopNAlgorithm only.
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
  public void updateResults(TopNResultBuilder resultBuilder)
  {
    for (Map.Entry<?, Aggregator[]> entry : aggregatesStore.entrySet()) {
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

  @Override
  public void closeAggregators()
  {
    for (Aggregator[] aggregators : aggregatesStore.values()) {
      for (Aggregator agg : aggregators) {
        agg.close();
      }
    }
  }

  @Override
  public long scanAndAggregate(
      TopNQuery query,
      DimensionSelector selector,
      Cursor cursor,
      Aggregator[][] rowSelector
  )
  {
    final boolean notUnknown = selector.getValueCardinality() != DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    final boolean unique = capabilities.isDictionaryEncoded().and(capabilities.areDictionaryValuesUnique()).isTrue();
    // we must know cardinality to use array based aggregation
    // we check for uniquely dictionary encoded values because non-unique (meaning dictionary ids do not have a 1:1
    // relation with values) negates many of the benefits of array aggregation:
    // - if different dictionary ids map to the same value but dictionary ids are unique to that value (*:1), then
    //   array aggregation will be correct but will still have to potentially perform many map lookups and lose the
    //   performance benefit array aggregation is trying to provide
    // - in cases where the same dictionary ids map to different values (1:* or *:*), results can be entirely
    //   incorrect since an aggregator for a different value might be chosen from the array based on the re-used
    //   dictionary id
    if (notUnknown && unique) {
      return scanAndAggregateWithCardinalityKnown(query, cursor, selector, rowSelector);
    } else {
      return scanAndAggregateWithCardinalityUnknown(query, cursor, selector);
    }
  }

  @Override
  public void initAggregateStore()
  {
    this.aggregatesStore = new HashMap<>();
  }

  private long scanAndAggregateWithCardinalityKnown(
      TopNQuery query,
      Cursor cursor,
      DimensionSelector selector,
      Aggregator[][] rowSelector
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      final IndexedInts dimValues = selector.getRow();
      for (int i = 0, size = dimValues.size(); i < size; ++i) {
        final int dimIndex = dimValues.get(i);
        Aggregator[] aggs = rowSelector[dimIndex];
        if (aggs == null) {
          final Comparable<?> key = dimensionValueConverter.apply(selector.lookupName(dimIndex));
          aggs = aggregatesStore.computeIfAbsent(
              key,
              k -> BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs())
          );
          rowSelector[dimIndex] = aggs;
        }

        for (Aggregator aggregator : aggs) {
          aggregator.aggregate();
        }
      }
      cursor.advance();
      processedRows++;
    }
    return processedRows;
  }

  private long scanAndAggregateWithCardinalityUnknown(
      TopNQuery query,
      Cursor cursor,
      DimensionSelector selector
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      final IndexedInts dimValues = selector.getRow();
      for (int i = 0, size = dimValues.size(); i < size; ++i) {
        final int dimIndex = dimValues.get(i);
        final Comparable<?> key = dimensionValueConverter.apply(selector.lookupName(dimIndex));
        Aggregator[] aggs = aggregatesStore.computeIfAbsent(
            key,
            k -> BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs())
        );
        for (Aggregator aggregator : aggs) {
          aggregator.aggregate();
        }
      }
      cursor.advance();
      processedRows++;
    }
    return processedRows;
  }
}
