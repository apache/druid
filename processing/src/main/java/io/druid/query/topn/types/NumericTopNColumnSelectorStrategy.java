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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Map;

public abstract class NumericTopNColumnSelectorStrategy<
    ValueSelectorType extends ColumnValueSelector,
    DimExtractionAggregateStoreType extends Map<?, Aggregator[]>>
    implements TopNColumnSelectorStrategy<ValueSelectorType, DimExtractionAggregateStoreType>
{
  @Override
  public int getCardinality(ValueSelectorType selector)
  {
    return TopNColumnSelectorStrategy.CARDINALITY_UNKNOWN;
  }

  @Override
  public Aggregator[][] getDimExtractionRowSelector(
      TopNQuery query, TopNParams params, Capabilities capabilities
  )
  {
    return null;
  }

  static long floatDimExtractionScanAndAggregate(
      TopNQuery query,
      FloatColumnSelector selector,
      Cursor cursor,
      Int2ObjectMap<Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      int key = Float.floatToIntBits(selector.get());
      Aggregator[] theAggregators = aggregatesStore.get(key);
      if (theAggregators == null) {
        theAggregators = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
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

  static long doubleDimExtractionScanAndAggregate(
      TopNQuery query,
      DoubleColumnSelector selector,
      Cursor cursor,
      Long2ObjectMap<Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      long key = Double.doubleToLongBits(selector.get());
      Aggregator[] theAggregators = aggregatesStore.get(key);
      if (theAggregators == null) {
        theAggregators = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
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

  static long longDimExtractionScanAndAggregate(
      TopNQuery query,
      LongColumnSelector selector,
      Cursor cursor,
      Long2ObjectMap<Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
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
      processedRows++;
    }
    return processedRows;
  }

  @Override
  public void updateDimExtractionResults(
      final DimExtractionAggregateStoreType aggregatesStore,
      final Function<Object, Object> valueTransformer,
      final TopNResultBuilder resultBuilder
  )
  {
    for (Map.Entry<?, Aggregator[]> entry : aggregatesStore.entrySet()) {
      Aggregator[] aggs = entry.getValue();
      if (aggs != null) {
        Object[] vals = new Object[aggs.length];
        for (int i = 0; i < aggs.length; i++) {
          vals[i] = aggs[i].get();
        }

        Comparable key = convertAggregatorStoreKeyToColumnValue(entry.getKey());
        if (valueTransformer != null) {
          key = (Comparable) valueTransformer.apply(key);
        }

        resultBuilder.addEntry(key, key, vals);
      }
    }
  }

  abstract Comparable convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey);

  static class OfFloat extends NumericTopNColumnSelectorStrategy<FloatColumnSelector, Int2ObjectMap<Aggregator[]>>
  {
    @Override
    public ValueType getValueType()
    {
      return ValueType.FLOAT;
    }

    @Override
    public Int2ObjectMap<Aggregator[]> makeDimExtractionAggregateStore()
    {
      return new Int2ObjectOpenHashMap<>();
    }

    @Override
    Comparable convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey)
    {
      return Float.intBitsToFloat((Integer) aggregatorStoreKey);
    }

    @Override
    public long dimExtractionScanAndAggregate(
        TopNQuery query,
        FloatColumnSelector selector,
        Cursor cursor,
        Aggregator[][] rowSelector,
        Int2ObjectMap<Aggregator[]> aggregatesStore
    )
    {
      return floatDimExtractionScanAndAggregate(query, selector, cursor, aggregatesStore);
    }
  }

  static class OfLong extends NumericTopNColumnSelectorStrategy<LongColumnSelector, Long2ObjectMap<Aggregator[]>>
  {
    @Override
    public ValueType getValueType()
    {
      return ValueType.LONG;
    }

    @Override
    public Long2ObjectMap<Aggregator[]> makeDimExtractionAggregateStore()
    {
      return new Long2ObjectOpenHashMap<>();
    }

    @Override
    Comparable convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey)
    {
      return (Long) aggregatorStoreKey;
    }

    @Override
    public long dimExtractionScanAndAggregate(
        TopNQuery query,
        LongColumnSelector selector,
        Cursor cursor,
        Aggregator[][] rowSelector,
        Long2ObjectMap<Aggregator[]> aggregatesStore
    )
    {
      return longDimExtractionScanAndAggregate(query, selector, cursor, aggregatesStore);
    }
  }

  static class OfDouble extends NumericTopNColumnSelectorStrategy<DoubleColumnSelector, Long2ObjectMap<Aggregator[]>>
  {
    @Override
    public ValueType getValueType()
    {
      return ValueType.DOUBLE;
    }

    @Override
    public Long2ObjectMap<Aggregator[]> makeDimExtractionAggregateStore()
    {
      return new Long2ObjectOpenHashMap<>();
    }

    @Override
    Comparable convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey)
    {
      return Double.longBitsToDouble((Long) aggregatorStoreKey);
    }

    @Override
    public long dimExtractionScanAndAggregate(
        TopNQuery query,
        DoubleColumnSelector selector,
        Cursor cursor,
        Aggregator[][] rowSelector,
        Long2ObjectMap<Aggregator[]> aggregatesStore
    )
    {
      return doubleDimExtractionScanAndAggregate(query, selector, cursor, aggregatesStore);
    }
  }
}
