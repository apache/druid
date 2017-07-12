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

package io.druid.query.topn;

import com.google.common.base.Function;
import io.druid.java.util.common.IAE;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.Result;
import io.druid.query.topn.types.TopNColumnSelectorStrategyFactory;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Objects;

public class TopNMapFn
{
  public static Function<Object, Object> getValueTransformer(ValueType outputType)
  {
    switch (outputType) {
      case STRING:
        return STRING_TRANSFORMER;
      case LONG:
        return LONG_TRANSFORMER;
      case FLOAT:
        return FLOAT_TRANSFORMER;
      case DOUBLE:
        return DOUBLE_TRANSFORMER;
      default:
        throw new IAE("invalid type: %s", outputType);
    }
  }

  private static Function<Object, Object> STRING_TRANSFORMER = input -> Objects.toString(input, null);

  private static Function<Object, Object> LONG_TRANSFORMER = input -> {
    final Long longVal = DimensionHandlerUtils.convertObjectToLong(input);
    return longVal == null ? DimensionHandlerUtils.ZERO_LONG : longVal;
  };

  private static Function<Object, Object> FLOAT_TRANSFORMER = input -> {
    final Float floatVal = DimensionHandlerUtils.convertObjectToFloat(input);
    return floatVal == null ? DimensionHandlerUtils.ZERO_FLOAT : floatVal;
  };
  private static Function<Object, Object> DOUBLE_TRANSFORMER = input -> {
    final Double doubleValue = DimensionHandlerUtils.convertObjectToDouble(input);
    return doubleValue == null ? DimensionHandlerUtils.ZERO_DOUBLE : doubleValue;
  };

  private static final TopNColumnSelectorStrategyFactory STRATEGY_FACTORY = new TopNColumnSelectorStrategyFactory();

  private final TopNQuery query;
  private final TopNAlgorithm topNAlgorithm;

  public TopNMapFn(
      TopNQuery query,
      TopNAlgorithm topNAlgorithm
  )
  {
    this.query = query;
    this.topNAlgorithm = topNAlgorithm;
  }

  @SuppressWarnings("unchecked")
  public Result<TopNResultValue> apply(final Cursor cursor, final @Nullable TopNQueryMetrics queryMetrics)
  {
    final ColumnSelectorPlus selectorPlus = DimensionHandlerUtils.createColumnSelectorPlus(
        STRATEGY_FACTORY,
        query.getDimensionSpec(),
        cursor
    );

    if (selectorPlus.getSelector() == null) {
      return null;
    }

    TopNParams params = null;
    try {
      params = topNAlgorithm.makeInitParams(selectorPlus, cursor);
      if (queryMetrics != null) {
        queryMetrics.columnValueSelector(selectorPlus.getSelector());
        queryMetrics.numValuesPerPass(params);
      }

      TopNResultBuilder resultBuilder = BaseTopNAlgorithm.makeResultBuilder(params, query);

      topNAlgorithm.run(params, resultBuilder, null, queryMetrics);

      return resultBuilder.build();
    }
    finally {
      topNAlgorithm.cleanup(params);
    }
  }
}
