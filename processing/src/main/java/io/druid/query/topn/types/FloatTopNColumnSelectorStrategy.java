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

import io.druid.query.aggregation.Aggregator;
import io.druid.query.topn.BaseTopNAlgorithm;
import io.druid.query.topn.TopNParams;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.FloatColumnSelector;

import java.util.Map;

public class FloatTopNColumnSelectorStrategy implements TopNColumnSelectorStrategy<FloatColumnSelector>
{
  @Override
  public int getCardinality(FloatColumnSelector selector)
  {
    return -1;
  }

  @Override
  public Aggregator[][] getDimExtractionRowSelector(
      TopNQuery query, TopNParams params, Capabilities capabilities
  )
  {
    return null;
  }

  @Override
  public void dimExtractionScanAndAggregate(
      TopNQuery query,
      FloatColumnSelector selector,
      Cursor cursor,
      Aggregator[][] rowSelector,
      Map<Comparable, Aggregator[]> aggregatesStore
  )
  {
    float key = selector.get();
    Aggregator[] theAggregators = aggregatesStore.get(key);
    if (theAggregators == null) {
      theAggregators = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
      aggregatesStore.put(key, theAggregators);
    }
    for (Aggregator aggregator : theAggregators) {
      aggregator.aggregate();
    }
  }
}
