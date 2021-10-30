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
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

import java.util.function.Function;

public class TopNColumnAggregatesProcessorFactory
    implements ColumnSelectorStrategyFactory<TopNColumnAggregatesProcessor<?>>
{
  private final ColumnType dimensionType;

  public TopNColumnAggregatesProcessorFactory(final ColumnType dimensionType)
  {
    this.dimensionType = Preconditions.checkNotNull(dimensionType, "dimensionType");
  }

  @Override
  public TopNColumnAggregatesProcessor<?> makeColumnSelectorStrategy(
      ColumnCapabilities capabilities,
      ColumnValueSelector selector
  )
  {
    if (capabilities.is(ValueType.STRING)) {
      return new StringTopNColumnAggregatesProcessor(capabilities, dimensionType);
    } else if (capabilities.isNumeric()) {
      final Function<Object, Comparable<?>> converter;
      final ColumnType strategyType;
      // When the selector is numeric, we want to use NumericTopNColumnSelectorStrategy. It aggregates using
      // a numeric type and then converts to the desired output type after aggregating. We must be careful not to
      // convert to an output type that cannot represent all possible values of the input type.
      if (dimensionType.isNumeric()) {
        // Return strategy that aggregates using the _output_ type, because this allows us to collapse values
        // properly (numeric types cannot always represent all values of other numeric types).
        converter = DimensionHandlerUtils.converterFromTypeToType(dimensionType, dimensionType);
        strategyType = dimensionType;
      } else {
        // Return strategy that aggregates using the _input_ type. Here we are assuming that the output type can
        // represent all possible values of the input type. This will be true for STRING, which is the only
        // non-numeric type currently supported.
        converter = DimensionHandlerUtils.converterFromTypeToType(capabilities, dimensionType);
        strategyType = capabilities.toColumnType();
      }
      switch (strategyType.getType()) {
        case LONG:
          return new LongTopNColumnAggregatesProcessor(converter);
        case FLOAT:
          return new FloatTopNColumnAggregatesProcessor(converter);
        case DOUBLE:
          return new DoubleTopNColumnAggregatesProcessor(converter);
      }
    }

    throw new IAE("Cannot create query type helper from invalid type [%s]", capabilities.asTypeString());
  }
}
