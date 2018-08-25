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

import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.topn.BaseTopNAlgorithm;
import io.druid.query.topn.TopNParams;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultBuilder;
import io.druid.segment.BaseDoubleColumnValueSelector;
import io.druid.segment.BaseFloatColumnValueSelector;
import io.druid.segment.BaseLongColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.ValueType;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Map;
import java.util.function.Function;

public abstract class NumericTopNColumnSelectorStrategy<
    ValueSelectorType,
    DimExtractionAggregateStoreType extends Map<?, Aggregator[]>>
    implements TopNColumnSelectorStrategy<ValueSelectorType, DimExtractionAggregateStoreType>
{
  public static TopNColumnSelectorStrategy ofType(final ValueType selectorType, final ValueType dimensionType)
  {
    final Function<Object, Comparable<?>> converter = DimensionHandlerUtils.converterFromTypeToTypeNonNull(
        selectorType,
        dimensionType
    );

    switch (selectorType) {
      case LONG:
        return new OfLong(converter);
      case FLOAT:
        return new OfFloat(converter);
      case DOUBLE:
        return new OfDouble(converter);
      default:
        throw new IAE("No strategy for type[%s]", selectorType);
    }
  }

  @Override
  public int getCardinality(ValueSelectorType selector)
  {
    return TopNColumnSelectorStrategy.CARDINALITY_UNKNOWN;
  }

  @Override
  public Aggregator[][] getDimExtractionRowSelector(
      TopNQuery query, TopNParams params, StorageAdapter storageAdapter
  )
  {
    return null;
  }

  static long floatDimExtractionScanAndAggregate(
      TopNQuery query,
      BaseFloatColumnValueSelector selector,
      Cursor cursor,
      Int2ObjectMap<Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      int key = Float.floatToIntBits(selector.getFloat());
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
      BaseDoubleColumnValueSelector selector,
      Cursor cursor,
      Long2ObjectMap<Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      long key = Double.doubleToLongBits(selector.getDouble());
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
      BaseLongColumnValueSelector selector,
      Cursor cursor,
      Long2ObjectMap<Aggregator[]> aggregatesStore
  )
  {
    long processedRows = 0;
    while (!cursor.isDone()) {
      long key = selector.getLong();
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

        final Comparable key = convertAggregatorStoreKeyToColumnValue(entry.getKey());
        resultBuilder.addEntry(key, key, vals);
      }
    }
  }

  abstract Comparable convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey);

  static class OfFloat
      extends NumericTopNColumnSelectorStrategy<BaseFloatColumnValueSelector, Int2ObjectMap<Aggregator[]>>
  {
    private final Function<Object, Comparable<?>> converter;

    OfFloat(final Function<Object, Comparable<?>> converter)
    {
      this.converter = converter;
    }

    @Override
    public Int2ObjectMap<Aggregator[]> makeDimExtractionAggregateStore()
    {
      return new Int2ObjectOpenHashMap<>();
    }

    @Override
    Comparable convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey)
    {
      return converter.apply(Float.intBitsToFloat((Integer) aggregatorStoreKey));
    }

    @Override
    public long dimExtractionScanAndAggregate(
        TopNQuery query,
        BaseFloatColumnValueSelector selector,
        Cursor cursor,
        Aggregator[][] rowSelector,
        Int2ObjectMap<Aggregator[]> aggregatesStore
    )
    {
      return floatDimExtractionScanAndAggregate(query, selector, cursor, aggregatesStore);
    }
  }

  static class OfLong
      extends NumericTopNColumnSelectorStrategy<BaseLongColumnValueSelector, Long2ObjectMap<Aggregator[]>>
  {
    private final Function<Object, Comparable<?>> converter;

    OfLong(final Function<Object, Comparable<?>> converter)
    {
      this.converter = converter;
    }

    @Override
    public Long2ObjectMap<Aggregator[]> makeDimExtractionAggregateStore()
    {
      return new Long2ObjectOpenHashMap<>();
    }

    @Override
    Comparable convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey)
    {
      return converter.apply(aggregatorStoreKey);
    }

    @Override
    public long dimExtractionScanAndAggregate(
        TopNQuery query,
        BaseLongColumnValueSelector selector,
        Cursor cursor,
        Aggregator[][] rowSelector,
        Long2ObjectMap<Aggregator[]> aggregatesStore
    )
    {
      return longDimExtractionScanAndAggregate(query, selector, cursor, aggregatesStore);
    }
  }

  static class OfDouble
      extends NumericTopNColumnSelectorStrategy<BaseDoubleColumnValueSelector, Long2ObjectMap<Aggregator[]>>
  {
    private final Function<Object, Comparable<?>> converter;

    OfDouble(final Function<Object, Comparable<?>> converter)
    {
      this.converter = converter;
    }

    @Override
    public Long2ObjectMap<Aggregator[]> makeDimExtractionAggregateStore()
    {
      return new Long2ObjectOpenHashMap<>();
    }

    @Override
    Comparable convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey)
    {
      return converter.apply(Double.longBitsToDouble((Long) aggregatorStoreKey));
    }

    @Override
    public long dimExtractionScanAndAggregate(
        TopNQuery query,
        BaseDoubleColumnValueSelector selector,
        Cursor cursor,
        Aggregator[][] rowSelector,
        Long2ObjectMap<Aggregator[]> aggregatesStore
    )
    {
      return doubleDimExtractionScanAndAggregate(query, selector, cursor, aggregatesStore);
    }
  }
}
