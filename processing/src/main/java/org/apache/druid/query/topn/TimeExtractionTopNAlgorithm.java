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

package org.apache.druid.query.topn;

import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TimeExtractionTopNAlgorithm extends BaseTopNAlgorithm<int[], Map<Object, Aggregator[]>, TopNParams>
{
  private static final int[] EMPTY_INTS = new int[]{};

  private final TopNQuery query;
  private final Function<Object, Object> dimensionValueConverter;

  public TimeExtractionTopNAlgorithm(StorageAdapter storageAdapter, TopNQuery query)
  {
    super(storageAdapter);
    this.query = query;

    // This strategy is used for ExtractionFns on the __time column. They always return STRING, so we need to convert
    // from STRING to the desired output type.
    this.dimensionValueConverter = DimensionHandlerUtils.converterFromTypeToType(
        ColumnType.STRING,
        query.getDimensionSpec().getOutputType()
    );
  }

  @Override
  @SuppressWarnings("unchecked")
  public TopNParams makeInitParams(ColumnSelectorPlus selectorPlus, Cursor cursor)
  {
    return new TopNParams(
        selectorPlus,
        cursor,
        Integer.MAX_VALUE
    );
  }

  @Override
  protected int[] makeDimValSelector(TopNParams params, int numProcessed, int numToProcess)
  {
    return EMPTY_INTS;
  }

  @Override
  protected int[] updateDimValSelector(int[] dimValSelector, int numProcessed, int numToProcess)
  {
    return dimValSelector;
  }

  @Override
  protected Map<Object, Aggregator[]> makeDimValAggregateStore(TopNParams params)
  {
    return new HashMap<>();
  }

  @Override
  protected long scanAndAggregate(
      TopNParams params,
      int[] dimValSelector,
      Map<Object, Aggregator[]> aggregatesStore
  )
  {
    final Cursor cursor = params.getCursor();
    final DimensionSelector dimSelector = params.getDimSelector();

    long processedRows = 0;
    while (!cursor.isDone()) {
      final Object key = dimensionValueConverter.apply(dimSelector.lookupName(dimSelector.getRow().get(0)));

      Aggregator[] theAggregators = aggregatesStore.computeIfAbsent(
          key,
          k -> makeAggregators(cursor, query.getAggregatorSpecs())
      );

      for (Aggregator aggregator : theAggregators) {
        aggregator.aggregate();
      }

      cursor.advance();
      processedRows++;
    }
    return processedRows;
  }

  @Override
  protected void updateResults(
      TopNParams params,
      int[] dimValSelector,
      Map<Object, Aggregator[]> aggregatesStore,
      TopNResultBuilder resultBuilder
  )
  {
    for (Map.Entry<Object, Aggregator[]> entry : aggregatesStore.entrySet()) {
      Aggregator[] aggs = entry.getValue();
      if (aggs != null) {
        Object[] vals = new Object[aggs.length];
        for (int i = 0; i < aggs.length; i++) {
          vals[i] = aggs[i].get();
        }

        resultBuilder.addEntry(
            entry.getKey(),
            entry.getKey(),
            vals
        );
      }
    }
  }

  @Override
  protected void closeAggregators(Map<Object, Aggregator[]> stringMap)
  {
    for (Aggregator[] aggregators : stringMap.values()) {
      for (Aggregator agg : aggregators) {
        agg.close();
      }
    }
  }

  @Override
  public void cleanup(TopNParams params)
  {

  }
}
