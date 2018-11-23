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

package org.apache.druid.query.aggregation.cardinality.types;

import com.google.common.hash.Hasher;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.dimension.ColumnSelectorStrategy;

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
