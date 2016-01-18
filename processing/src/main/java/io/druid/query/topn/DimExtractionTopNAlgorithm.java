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
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.Map;

/**
 * This has to be its own strategy because the pooled topn algorithm assumes each index is unique, and cannot handle multiple index numerals referencing the same dimension value.
 */
public class DimExtractionTopNAlgorithm extends BaseTopNAlgorithm<Aggregator[][], Map<String, Aggregator[]>, TopNParams>
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
      final DimensionSelector dimSelector,
      final Cursor cursor
  )
  {
    return new TopNParams(
        dimSelector,
        cursor,
        dimSelector.getValueCardinality(),
        Integer.MAX_VALUE
    );
  }

  @Override
  protected Aggregator[][] makeDimValSelector(TopNParams params, int numProcessed, int numToProcess)
  {
    final AggregatorArrayProvider provider = new AggregatorArrayProvider(
        params.getDimSelector(),
        query,
        params.getCardinality()
    );

    // Unlike regular topN we cannot rely on ordering to optimize.
    // Optimization possibly requires a reverse lookup from value to ID, which is
    // not possible when applying an extraction function
    return provider.build();
  }

  @Override
  protected Aggregator[][] updateDimValSelector(Aggregator[][] aggregators, int numProcessed, int numToProcess)
  {
    return aggregators;
  }

  @Override
  protected Map<String, Aggregator[]> makeDimValAggregateStore(TopNParams params)
  {
    return Maps.newHashMap();
  }

  @Override
  public void scanAndAggregate(
      TopNParams params,
      Aggregator[][] rowSelector,
      Map<String, Aggregator[]> aggregatesStore,
      int numProcessed
  )
  {
    final Cursor cursor = params.getCursor();
    final DimensionSelector dimSelector = params.getDimSelector();

    while (!cursor.isDone()) {
      final IndexedInts dimValues = dimSelector.getRow();

      for (int i = 0; i < dimValues.size(); ++i) {

        final int dimIndex = dimValues.get(i);
        Aggregator[] theAggregators = rowSelector[dimIndex];
        if (theAggregators == null) {
          final String key = dimSelector.lookupName(dimIndex);
          theAggregators = aggregatesStore.get(key);
          if (theAggregators == null) {
            theAggregators = makeAggregators(cursor, query.getAggregatorSpecs());
            aggregatesStore.put(key, theAggregators);
          }
          rowSelector[dimIndex] = theAggregators;
        }

        for (Aggregator aggregator : theAggregators) {
          aggregator.aggregate();
        }
      }

      cursor.advance();
    }
  }

  @Override
  protected void updateResults(
      TopNParams params,
      Aggregator[][] rowSelector,
      Map<String, Aggregator[]> aggregatesStore,
      TopNResultBuilder resultBuilder
  )
  {
    for (Map.Entry<String, Aggregator[]> entry : aggregatesStore.entrySet()) {
      Aggregator[] aggs = entry.getValue();
      if (aggs != null && aggs.length > 0) {
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
