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

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.topn.BaseTopNAlgorithm;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.Cursor;

import java.util.Map;
import java.util.function.Function;

public class DoubleTopNColumnAggregatesProcessor
    extends NullableNumericTopNColumnAggregatesProcessor<BaseDoubleColumnValueSelector>
{
  private Long2ObjectMap<Aggregator[]> aggregatesStore;

  protected DoubleTopNColumnAggregatesProcessor(Function<Object, Comparable<?>> converter)
  {
    super(converter);
  }

  @Override
  Aggregator[] getValueAggregators(
      TopNQuery query,
      BaseDoubleColumnValueSelector selector,
      Cursor cursor
  )
  {
    long key = Double.doubleToLongBits(selector.getDouble());
    Aggregator[] aggs = aggregatesStore.get(key);
    if (aggs == null) {
      aggs = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
      aggregatesStore.put(key, aggs);
    }
    return aggs;
  }

  @Override
  public void initAggregateStore()
  {
    nullValueAggregates = null;
    aggregatesStore = new Long2ObjectOpenHashMap<>();
  }

  @Override
  Map<?, Aggregator[]> getAggregatesStore()
  {
    return aggregatesStore;
  }

  @Override
  Comparable<?> convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey)
  {
    return converter.apply(Double.longBitsToDouble((Long) aggregatorStoreKey));
  }
}
