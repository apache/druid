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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;

public class TopNColumnSelectorStrategyFactory implements ColumnSelectorStrategyFactory<TopNColumnSelectorStrategy>
{
  private final ValueType dimensionType;

  public TopNColumnSelectorStrategyFactory(final ValueType dimensionType)
  {
    this.dimensionType = Preconditions.checkNotNull(dimensionType, "dimensionType");
  }

  @Override
  public TopNColumnSelectorStrategy makeColumnSelectorStrategy(
      ColumnCapabilities capabilities,
      ColumnValueSelector selector
  )
  {
    final ValueType selectorType = capabilities.getType();

    switch (selectorType) {
      case STRING:
        // Return strategy that reads strings and outputs dimensionTypes.
        return new StringTopNColumnSelectorStrategy(dimensionType);
      case LONG:
      case FLOAT:
      case DOUBLE:
        // When the selector is numeric, we want to use NumericTopNColumnSelectorStrategy. It aggregates using
        // a numeric type and then converts to the desired output type after aggregating. We must be careful not to
        // convert to an output type that cannot represent all possible values of the input type.

        if (ValueType.isNumeric(dimensionType)) {
          // Return strategy that aggregates using the _output_ type, because this allows us to collapse values
          // properly (numeric types cannot always represent all values of other numeric types).
          return NumericTopNColumnSelectorStrategy.ofType(dimensionType, dimensionType);
        } else {
          // Return strategy that aggregates using the _input_ type. Here we are assuming that the output type can
          // represent all possible values of the input type. This will be true for STRING, which is the only
          // non-numeric type currently supported.
          return NumericTopNColumnSelectorStrategy.ofType(selectorType, dimensionType);
        }
      default:
        throw new IAE("Cannot create query type helper from invalid type [%s]", selectorType);
    }
  }
}
