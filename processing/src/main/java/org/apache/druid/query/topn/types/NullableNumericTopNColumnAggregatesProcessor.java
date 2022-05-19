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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.topn.BaseTopNAlgorithm;
import org.apache.druid.query.topn.TopNParams;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNResultBuilder;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;

import java.util.Map;
import java.util.function.Function;

/**
 * Base {@link TopNColumnAggregatesProcessor} for {@link BaseNullableColumnValueSelector}. Non-null selector values
 * aggregates are stored in a type appropriate primitive map, created by {@link #initAggregateStore()} and available
 * via {@link #getAggregatesStore()}, and null valued row aggregates are stored in a separate
 * {@link #nullValueAggregates} {@link Aggregator} array.
 *
 * {@link #updateResults} will combine both the map and null aggregates to populate the {@link TopNResultBuilder} with
 * the values produced by {@link #scanAndAggregate}.
 */
public abstract class NullableNumericTopNColumnAggregatesProcessor<Selector extends BaseNullableColumnValueSelector>
    implements TopNColumnAggregatesProcessor<Selector>
{
  private final boolean hasNulls = !NullHandling.replaceWithDefault();
  final Function<Object, Comparable<?>> converter;
  Aggregator[] nullValueAggregates;

  protected NullableNumericTopNColumnAggregatesProcessor(Function<Object, Comparable<?>> converter)
  {
    this.converter = converter;
  }

  /**
   * Get {@link Aggregator} set for the current {@param Selector} row value for a given {@link Cursor}
   */
  abstract Aggregator[] getValueAggregators(TopNQuery query, Selector selector, Cursor cursor);

  /**
   * Get primitive numeric map for value aggregates created by {@link #scanAndAggregate}, to be used by
   * {@link #updateResults} to apply to the {@link TopNResultBuilder}
   */
  abstract Map<?, Aggregator[]> getAggregatesStore();

  /**
   * Method to convert primitive numeric value keys used by {@link #getAggregatesStore} into the correct representation
   * for the {@link TopNResultBuilder}, called by {@link #updateResults}
   */
  abstract Comparable<?> convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey);

  @Override
  public int getCardinality(Selector selector)
  {
    return TopNParams.CARDINALITY_UNKNOWN;
  }

  @Override
  public Aggregator[][] getRowSelector(TopNQuery query, TopNParams params, StorageAdapter storageAdapter)
  {
    return null;
  }

  @Override
  public long scanAndAggregate(
      TopNQuery query,
      Selector selector,
      Cursor cursor,
      Aggregator[][] rowSelector
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      if (hasNulls && selector.isNull()) {
        if (nullValueAggregates == null) {
          nullValueAggregates = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
        }
        for (Aggregator aggregator : nullValueAggregates) {
          aggregator.aggregate();
        }
      } else {
        Aggregator[] valueAggregates = getValueAggregators(query, selector, cursor);
        for (Aggregator aggregator : valueAggregates) {
          aggregator.aggregate();
        }
      }
      cursor.advance();
      processedRows++;
    }
    return processedRows;
  }


  @Override
  public void updateResults(TopNResultBuilder resultBuilder)
  {
    for (Map.Entry<?, Aggregator[]> entry : getAggregatesStore().entrySet()) {
      Aggregator[] aggs = entry.getValue();
      if (aggs != null) {
        Object[] vals = new Object[aggs.length];
        for (int i = 0; i < aggs.length; i++) {
          vals[i] = aggs[i].get();
        }

        final Comparable<?> key = convertAggregatorStoreKeyToColumnValue(entry.getKey());
        resultBuilder.addEntry(key, key, vals);
      }
    }

    if (nullValueAggregates != null) {
      Object[] nullVals = new Object[nullValueAggregates.length];
      for (int i = 0; i < nullValueAggregates.length; i++) {
        nullVals[i] = nullValueAggregates[i].get();
      }

      resultBuilder.addEntry(null, null, nullVals);
    }
  }

  @Override
  public void closeAggregators()
  {
    for (Aggregator[] aggregators : getAggregatesStore().values()) {
      for (Aggregator agg : aggregators) {
        agg.close();
      }
    }

    if (nullValueAggregates != null) {
      for (Aggregator nullAgg : nullValueAggregates) {
        nullAgg.close();
      }
    }
  }
}
