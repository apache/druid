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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.topn.BaseTopNAlgorithm;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.Cursor;

import java.util.Map;
import java.util.function.Function;

public class FloatTopNColumnAggregatesProcessor
    extends NullableNumericTopNColumnAggregatesProcessor<BaseFloatColumnValueSelector>
{
  private Int2ObjectMap<Aggregator[]> aggregatesStore;

  protected FloatTopNColumnAggregatesProcessor(Function<Object, Comparable<?>> converter)
  {
    super(converter);
  }

  @Override
  Aggregator[] getValueAggregators(
      TopNQuery query,
      BaseFloatColumnValueSelector selector,
      Cursor cursor
  )
  {
    int key = Float.floatToIntBits(selector.getFloat());
    return aggregatesStore.computeIfAbsent(
        key,
        k -> BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs())
    );
  }

  @Override
  public void initAggregateStore()
  {
    nullValueAggregates = null;
    this.aggregatesStore = new Int2ObjectOpenHashMap<>();
  }

  @Override
  Map<?, Aggregator[]> getAggregatesStore()
  {
    return aggregatesStore;
  }

  @Override
  Comparable<?> convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey)
  {
    return converter.apply(Float.intBitsToFloat((Integer) aggregatorStoreKey));
  }
}
