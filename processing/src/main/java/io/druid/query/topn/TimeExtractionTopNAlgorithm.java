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

import com.google.common.collect.Maps;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.ColumnSelectorPlus;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;

import java.util.Map;

public class TimeExtractionTopNAlgorithm extends BaseTopNAlgorithm<int[], Map<String, Aggregator[]>, TopNParams>
{
  public static final int[] EMPTY_INTS = new int[]{};
  private final TopNQuery query;

  public TimeExtractionTopNAlgorithm(Capabilities capabilities, TopNQuery query)
  {
    super(capabilities);
    this.query = query;
  }


  @Override
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
  protected Map<String, Aggregator[]> makeDimValAggregateStore(TopNParams params)
  {
    return Maps.newHashMap();
  }

  @Override
  protected long scanAndAggregate(
      TopNParams params, int[] dimValSelector, Map<String, Aggregator[]> aggregatesStore, int numProcessed
  )
  {
    if (params.getCardinality() < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with unknown cardinality");
    }

    final Cursor cursor = params.getCursor();
    final DimensionSelector dimSelector = params.getDimSelector();

    long processedRows = 0;
    while (!cursor.isDone()) {
      final String key = dimSelector.lookupName(dimSelector.getRow().get(0));

      Aggregator[] theAggregators = aggregatesStore.get(key);
      if (theAggregators == null) {
        theAggregators = makeAggregators(cursor, query.getAggregatorSpecs());
        aggregatesStore.put(key, theAggregators);
      }

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
      Map<String, Aggregator[]> aggregatesStore,
      TopNResultBuilder resultBuilder
  )
  {
    for (Map.Entry<String, Aggregator[]> entry : aggregatesStore.entrySet()) {
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
  protected void closeAggregators(Map<String, Aggregator[]> stringMap)
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
