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
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;

public class TopNColumnSelectorStrategyFactory implements ColumnSelectorStrategyFactory<TopNColumnSelectorStrategy>
{
  @Override
  public TopNColumnSelectorStrategy makeColumnSelectorStrategy(
      ColumnCapabilities capabilities, ColumnValueSelector selector
  )
  {
    ValueType type = capabilities.getType();
    switch(type) {
      case STRING:
        return new StringTopNColumnSelectorStrategy();
      case LONG:
        return new NumericTopNColumnSelectorStrategy.OfLong();
      case FLOAT:
        return new NumericTopNColumnSelectorStrategy.OfFloat();
      case DOUBLE:
        return new NumericTopNColumnSelectorStrategy.OfDouble();
      default:
        throw new IAE("Cannot create query type helper from invalid type [%s]", type);
    }
  }
}
