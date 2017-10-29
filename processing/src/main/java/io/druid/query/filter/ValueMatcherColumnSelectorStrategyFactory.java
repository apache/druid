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

package io.druid.query.filter;

import io.druid.java.util.common.IAE;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;

public class ValueMatcherColumnSelectorStrategyFactory
    implements ColumnSelectorStrategyFactory<ValueMatcherColumnSelectorStrategy>
{
  private static final ValueMatcherColumnSelectorStrategyFactory INSTANCE = new ValueMatcherColumnSelectorStrategyFactory();

  private ValueMatcherColumnSelectorStrategyFactory()
  {
    // Singleton.
  }

  public static ValueMatcherColumnSelectorStrategyFactory instance()
  {
    return INSTANCE;
  }

  @Override
  public ValueMatcherColumnSelectorStrategy makeColumnSelectorStrategy(
      ColumnCapabilities capabilities, ColumnValueSelector selector
  )
  {
    ValueType type = capabilities.getType();
    switch (type) {
      case STRING:
        return new StringValueMatcherColumnSelectorStrategy();
      case LONG:
        return new LongValueMatcherColumnSelectorStrategy();
      case FLOAT:
        return new FloatValueMatcherColumnSelectorStrategy();
      case DOUBLE:
        return new DoubleValueMatcherColumnSelectorStrategy();
      default:
        throw new IAE("Cannot create column selector strategy from invalid type [%s]", type);
    }
  }
}
