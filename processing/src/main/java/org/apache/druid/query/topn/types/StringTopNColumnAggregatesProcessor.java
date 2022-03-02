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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.IndexedInts;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class StringTopNColumnAggregatesProcessor implements TopNColumnAggregatesProcessor<DimensionSelector>
{
  private final ColumnCapabilities capabilities;
  private final Function<Object, Comparable<?>> dimensionValueConverter;
  private HashMap<Comparable<?>, Aggregator[]> aggregatesStore;

  public StringTopNColumnAggregatesProcessor(final ColumnCapabilities capabilities, final ColumnType dimensionType)
  {
    this.capabilities = capabilities;
    this.dimensionValueConverter = DimensionHandlerUtils.converterFromTypeToType(ColumnType.STRING, dimensionType);
  }

  @Override
  public int getCardinality(DimensionSelector selector)
  {
    // only report the underlying selector cardinality if the column the selector is for is dictionary encoded
    if (capabilities.isDictionaryEncoded().isTrue()) {
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
    final boolean hasDictionary = capabilities.isDictionaryEncoded().isTrue();
    // we must know cardinality to use array based aggregation. in cases where the same dictionary ids map to different
    // values (1:* or *:*), results can be entirely incorrect since an aggregator for a different value might be
    // chosen from the array based on the re-used dictionary id
    if (notUnknown && hasDictionary) {
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

  /**
   * scan and aggregate when column is dictionary encoded and value cardinality is known up front, so values are
   * aggregated into an array position specified by the dictionaryid, which if not already present, are translated
   * into the key and fetched (or created if they key hasn't been encountered) from the {@link #aggregatesStore}
   */
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

  /**
   * this method is to allow scan and aggregate when values are not dictionary encoded
   * (e.g. {@link DimensionSelector#nameLookupPossibleInAdvance()} is false and/or when
   * {@link ColumnCapabilities#isDictionaryEncoded()} is false). This mode also uses hash table aggregation, storing
   * results in {@link #aggregatesStore}, and must call {@link DimensionSelector#lookupName(int)} for every row which
   * is processed and cannot cache lookups, or use the dictionary id in any way other than to lookup the current row
   * value.
   */
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
