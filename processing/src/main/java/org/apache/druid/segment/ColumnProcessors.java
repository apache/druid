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

package org.apache.druid.segment;

import com.google.common.base.Function;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionSelectors;

import javax.annotation.Nullable;

/**
 * Creates "column processors", which are objects that wrap a single input column and provide some functionality on
 * top of it.
 *
 * @see DimensionHandlerUtils#createColumnSelectorPlus which this may eventually replace
 * @see DimensionHandlerUtils#makeVectorProcessor which creates similar, vectorized processors; may eventually be moved
 * into this class.
 */
public class ColumnProcessors
{
  /**
   * Make a processor for a particular named column.
   *
   * @param column           the column
   * @param processorFactory the processor factory
   * @param selectorFactory  the column selector factory
   * @param <T>              processor type
   */
  public static <T> T makeProcessor(
      final String column,
      final ColumnProcessorFactory<T> processorFactory,
      final ColumnSelectorFactory selectorFactory
  )
  {
    return makeProcessorInternal(
        factory -> getColumnType(factory, column),
        factory -> factory.makeDimensionSelector(DefaultDimensionSpec.of(column)),
        factory -> factory.makeColumnValueSelector(column),
        processorFactory,
        selectorFactory
    );
  }

  /**
   * Make a processor for a particular expression. If the expression is a simple identifier, this behaves identically
   * to {@link #makeProcessor(String, ColumnProcessorFactory, ColumnSelectorFactory)} and accesses the column directly.
   * Otherwise, it uses an expression selector of type {@code exprTypeHint}.
   *
   * @param expr             the parsed expression
   * @param processorFactory the processor factory
   * @param selectorFactory  the column selector factory
   * @param <T>              processor type
   */
  public static <T> T makeProcessor(
      final Expr expr,
      final ValueType exprTypeHint,
      final ColumnProcessorFactory<T> processorFactory,
      final ColumnSelectorFactory selectorFactory
  )
  {
    if (expr.getIdentifierIfIdentifier() != null) {
      // If expr is an identifier, treat this the same way as a direct column reference.
      return makeProcessor(expr.getIdentifierIfIdentifier(), processorFactory, selectorFactory);
    } else {
      return makeProcessorInternal(
          factory -> exprTypeHint,
          factory -> ExpressionSelectors.makeDimensionSelector(factory, expr, null),
          factory -> ExpressionSelectors.makeColumnValueSelector(factory, expr),
          processorFactory,
          selectorFactory
      );
    }
  }

  /**
   * Creates "column processors", which are objects that wrap a single input column and provide some
   * functionality on top of it.
   *
   * @param inputTypeFn           function that returns the "natural" input type of the column being processed. This is
   *                              permitted to return null; if it does, then processorFactory.defaultType() will be used.
   * @param dimensionSelectorFn   function that creates a DimensionSelector for the column being processed. Will be
   *                              called if the column type is string.
   * @param valueSelectorFunction function that creates a ColumnValueSelector for the column being processed. Will be
   *                              called if the column type is long, float, double, or complex.
   * @param processorFactory      object that encapsulates the knowledge about how to create processors
   * @param selectorFactory       column selector factory used for creating the vector processor
   *
   * @see DimensionHandlerUtils#makeVectorProcessor the vectorized version
   */
  private static <T> T makeProcessorInternal(
      final Function<ColumnSelectorFactory, ValueType> inputTypeFn,
      final Function<ColumnSelectorFactory, DimensionSelector> dimensionSelectorFn,
      final Function<ColumnSelectorFactory, ColumnValueSelector<?>> valueSelectorFunction,
      final ColumnProcessorFactory<T> processorFactory,
      final ColumnSelectorFactory selectorFactory
  )
  {
    final ValueType type = inputTypeFn.apply(selectorFactory);
    final ValueType effectiveType = type != null ? type : processorFactory.defaultType();

    switch (effectiveType) {
      case STRING:
        return processorFactory.makeDimensionProcessor(dimensionSelectorFn.apply(selectorFactory));
      case LONG:
        return processorFactory.makeLongProcessor(valueSelectorFunction.apply(selectorFactory));
      case FLOAT:
        return processorFactory.makeFloatProcessor(valueSelectorFunction.apply(selectorFactory));
      case DOUBLE:
        return processorFactory.makeDoubleProcessor(valueSelectorFunction.apply(selectorFactory));
      case COMPLEX:
        return processorFactory.makeComplexProcessor(valueSelectorFunction.apply(selectorFactory));
      default:
        throw new ISE("Unsupported type[%s]", effectiveType);
    }
  }

  @Nullable
  private static ValueType getColumnType(final ColumnSelectorFactory selectorFactory, final String columnName)
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    return capabilities == null ? null : capabilities.getType();
  }
}
