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
import io.druid.hll.HyperLogLogCollector;
import io.druid.query.dimension.ColumnSelectorStrategy;

public interface CardinalityAggregatorColumnSelectorStrategy<ValueSelectorType> extends ColumnSelectorStrategy
{
  /***
   * Retrieve the current row from dimSelector and add the row values to the hasher.
   *
   * @param dimSelector Dimension value selector
   * @param hasher Hasher used for cardinality aggregator calculations
   */
  void hashRow(ValueSelectorType dimSelector, Hasher hasher);


  /**
   * Retrieve the current row from dimSelector and add the row values to HyperLogLogCollector.
   *
   * @param dimSelector Dimension value selector
   * @param collector HLL collector used for cardinality aggregator calculations
   */
  void hashValues(ValueSelectorType dimSelector, HyperLogLogCollector collector);
}
