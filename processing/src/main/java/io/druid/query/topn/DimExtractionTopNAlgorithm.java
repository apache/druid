/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

import com.google.common.collect.Maps;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.Comparator;
import java.util.Map;

/**
 */
public class DimExtractionTopNAlgorithm extends BaseTopNAlgorithm<Aggregator[][], Map<String, Aggregator[]>, TopNParams>
{
  private final TopNQuery query;
  private final Comparator<?> comparator;

  public DimExtractionTopNAlgorithm(
      Capabilities capabilities,
      TopNQuery query
  )
  {
    super(capabilities);

    this.query = query;
    this.comparator = query.getTopNMetricSpec()
                           .getComparator(query.getAggregatorSpecs(), query.getPostAggregatorSpecs());
  }

  @Override
  public TopNParams makeInitParams(
      final DimensionSelector dimSelector, final Cursor cursor
  )
  {
    return new TopNParams(dimSelector, cursor, dimSelector.getValueCardinality(), Integer.MAX_VALUE);
  }

  @Override
  protected Aggregator[][] makeDimValSelector(TopNParams params, int numProcessed, int numToProcess)
  {
    return query.getTopNMetricSpec().configureOptimizer(
        new AggregatorArrayProvider(params.getDimSelector(), query, params.getCardinality())
    ).build();
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
          String key = query.getDimensionSpec().getDimExtractionFn().apply(dimSelector.lookupName(dimIndex));
          if (key == null) {
            rowSelector[dimIndex] = EMPTY_ARRAY;
            continue;
          }
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
