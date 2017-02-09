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
import io.druid.query.aggregation.Aggregator;
import io.druid.query.topn.BaseTopNAlgorithm;
import io.druid.query.topn.TopNParams;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultBuilder;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.column.ValueType;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class LongTopNColumnSelectorStrategy
    implements TopNColumnSelectorStrategy<LongColumnSelector, Long2ObjectMap<Aggregator[]>>
{
  @Override
  public int getCardinality(LongColumnSelector selector)
  {
    return TopNColumnSelectorStrategy.CARDINALITY_UNKNOWN;
  }

  @Override
  public ValueType getValueType()
  {
    return ValueType.LONG;
  }

  @Override
  public Aggregator[][] getDimExtractionRowSelector(
      TopNQuery query, TopNParams params, Capabilities capabilities
  )
  {
    return null;
  }

  @Override
  public Long2ObjectMap<Aggregator[]> makeDimExtractionAggregateStore()
  {
    return new Long2ObjectOpenHashMap<>();
  }

  @Override
  public void dimExtractionScanAndAggregate(
      TopNQuery query,
      LongColumnSelector selector,
      Cursor cursor,
      Aggregator[][] rowSelector,
      Long2ObjectMap<Aggregator[]> aggregatesStore
  )
  {
    while (!cursor.isDone()) {
      long key = selector.get();
      Aggregator[] theAggregators = aggregatesStore.get(key);
      if (theAggregators == null) {
        theAggregators = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
        aggregatesStore.put(key, theAggregators);
      }
      for (Aggregator aggregator : theAggregators) {
        aggregator.aggregate();
      }
      cursor.advance();
    }
  }

  @Override
  public void updateDimExtractionResults(
      final Long2ObjectMap<Aggregator[]> aggregatesStore,
      final Function<Object, Object> valueTransformer,
      final TopNResultBuilder resultBuilder
  )
  {
    for (Long2ObjectMap.Entry<Aggregator[]> entry : aggregatesStore.long2ObjectEntrySet()) {
      Aggregator[] aggs = entry.getValue();
      if (aggs != null && aggs.length > 0) {
        Object[] vals = new Object[aggs.length];
        for (int i = 0; i < aggs.length; i++) {
          vals[i] = aggs[i].get();
        }

        Comparable key = entry.getLongKey();
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
}
