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
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.column.ValueType;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import javax.annotation.Nullable;

public class NumericTopNColumnSelectorStrategy<ValueSelectorType extends ColumnValueSelector>
    implements TopNColumnSelectorStrategy<ValueSelectorType, Long2ObjectMap<Aggregator[]>>
{
  private final ValueType valueType;
  private final Function<ValueSelectorType, Long> keyFn;
  private final Function<Long, Comparable> reverseKeyFn;

  public NumericTopNColumnSelectorStrategy(ValueType valueType)
  {
    this.valueType = valueType;
    keyFn = input -> {
      switch (valueType) {
        case LONG:
          return ((LongColumnSelector) input).get();
        case FLOAT:
          return Double.doubleToLongBits((double) ((FloatColumnSelector) input).get());
        case DOUBLE:
          return Double.doubleToLongBits(((DoubleColumnSelector) input).get());
        default:
          throw new UnsupportedOperationException("should not be here supports only numeric types");
      }
    };
    reverseKeyFn = input -> {
      switch (valueType) {
        case LONG:
          return input;
        case FLOAT:
        case DOUBLE:
          return Double.longBitsToDouble(input);
        default:
          throw new UnsupportedOperationException("should not be here supports only numeric types");
      }
    };
  }

  @Override
  public int getCardinality(ValueSelectorType selector)
  {
    return TopNColumnSelectorStrategy.CARDINALITY_UNKNOWN;
  }


  @Override
  public ValueType getValueType()
  {
    return valueType;
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
  public long dimExtractionScanAndAggregate(
      TopNQuery query,
      ValueSelectorType selector,
      Cursor cursor,
      Aggregator[][] rowSelector,
      Long2ObjectMap<Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      long key = keyFn.apply(selector);
      Aggregator[] aggregators = aggregatesStore.get(key);
      if (aggregators == null) {
        aggregators = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
        aggregatesStore.put(key, aggregators);
      }
      for (Aggregator aggregator : aggregators) {
        aggregator.aggregate();
      }
      cursor.advance();
      processedRows++;
    }
    return processedRows;
  }

  @Override
  public void updateDimExtractionResults(
      Long2ObjectMap<Aggregator[]> aggregatesStore,
      @Nullable Function<Object, Object> valueTransformer,
      TopNResultBuilder resultBuilder
  )
  {
    for (Long2ObjectMap.Entry<Aggregator[]> entry : aggregatesStore.long2ObjectEntrySet()) {
      Aggregator[] aggs = entry.getValue();
      if (aggs != null) {
        Object[] vals = new Object[aggs.length];
        for (int i = 0; i < aggs.length; i++) {
          vals[i] = aggs[i].get();
        }
        Comparable key = reverseKeyFn.apply(entry.getLongKey());
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
