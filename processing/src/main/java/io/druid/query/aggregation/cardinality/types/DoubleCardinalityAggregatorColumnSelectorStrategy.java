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

package io.druid.query.aggregation.cardinality.types;

import com.google.common.hash.Hasher;
import io.druid.common.config.NullHandling;
import io.druid.hll.HyperLogLogCollector;
import io.druid.query.aggregation.cardinality.CardinalityAggregator;
import io.druid.segment.BaseDoubleColumnValueSelector;

/**
 * If performance of this class appears to be a bottleneck for somebody,
 * one simple way to improve it is to split it into two different classes,
 * one that is used when {@link NullHandling#replaceWithDefault()} is false,
 * and one - when it's true, moving this computation out of the tight loop
 */
public class DoubleCardinalityAggregatorColumnSelectorStrategy
    implements CardinalityAggregatorColumnSelectorStrategy<BaseDoubleColumnValueSelector>
{
  @Override
  public void hashRow(BaseDoubleColumnValueSelector selector, Hasher hasher)
  {
    if (NullHandling.replaceWithDefault() || !selector.isNull()) {
      hasher.putDouble(selector.getDouble());
    }
  }

  @Override
  public void hashValues(BaseDoubleColumnValueSelector selector, HyperLogLogCollector collector)
  {
    if (NullHandling.replaceWithDefault() || !selector.isNull()) {
      collector.add(CardinalityAggregator.hashFn.hashLong(Double.doubleToLongBits(selector.getDouble())).asBytes());
    }
  }
}
