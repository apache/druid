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

package org.apache.druid.query.aggregation.cardinality.accurate.types;


import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;

public class AccurateCardinalityAggregatorColumnSelectorStrategyFactory
    implements ColumnSelectorStrategyFactory<AccurateCardinalityAggregatorColumnSelectorStrategy>
{
  @Override
  public AccurateCardinalityAggregatorColumnSelectorStrategy makeColumnSelectorStrategy(
      ColumnCapabilities capabilities,
      ColumnValueSelector selector
  )
  {
    ValueType type = capabilities.getType();
    switch (type) {
      case LONG:
        return new LongAccurateCardinalityAggregatorColumnSelectorStrategy();
      case STRING:
        return new StringAccurateCardinalityAggregatorColumnSelectorStrategy();
      case FLOAT:
        throw new IAE("Query type helper doesn't support type[%s]", type);
      default:
        throw new IAE("Cannot create query type helper from invalid type [%s]", type);
    }
  }
}
