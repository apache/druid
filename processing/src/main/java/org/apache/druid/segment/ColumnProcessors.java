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
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
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
        factory -> factory.getColumnCapabilities(column),
        factory -> factory.makeDimensionSelector(DefaultDimensionSpec.of(column)),
        factory -> factory.makeColumnValueSelector(column),
        processorFactory,
        selectorFactory
    );
  }

  /**
   * Make a processor for a particular {@link DimensionSpec}.
   *
   * @param dimensionSpec    the dimension spec
   * @param processorFactory the processor factory
   * @param selectorFactory  the column selector factory
   * @param <T>              processor type
   */
  public static <T> T makeProcessor(
      final DimensionSpec dimensionSpec,
      final ColumnProcessorFactory<T> processorFactory,
      final ColumnSelectorFactory selectorFactory
  )
  {
    return makeProcessorInternal(
        factory -> {
          // Capabilities of the column that the dimensionSpec is reading. We can't return these straight-up, because
          // the _result_ of the dimensionSpec might have different capabilities. But what we return will generally be
          // based on them.
          final ColumnCapabilities dimensionCapabilities = factory.getColumnCapabilities(dimensionSpec.getDimension());

          if (dimensionSpec.getExtractionFn() != null || dimensionSpec.mustDecorate()) {
            // DimensionSpec is doing some sort of transformation. The result is always a string.

            return new ColumnCapabilitiesImpl()
                .setType(ValueType.STRING)
                .setDictionaryValuesSorted(dimensionSpec.getExtractionFn().preservesOrdering())
                .setDictionaryValuesUnique(dimensionSpec.getExtractionFn().getExtractionType() == ExtractionFn.ExtractionType.ONE_TO_ONE)
                .setHasMultipleValues(dimensionSpec.mustDecorate() || mayBeMultiValue(dimensionCapabilities));
          } else {
            // No transformation. Pass through.
            return dimensionCapabilities;
          }
        },
        factory -> factory.makeDimensionSelector(dimensionSpec),
        factory -> factory.makeColumnValueSelector(dimensionSpec.getDimension()),
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
   * @param exprTypeHint     expression selector type to use for exprs that are not simple identifiers
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
    Preconditions.checkNotNull(exprTypeHint, "'exprTypeHint' must be nonnull");

    if (expr.getBindingIfIdentifier() != null) {
      // If expr is an identifier, treat this the same way as a direct column reference.
      return makeProcessor(expr.getBindingIfIdentifier(), processorFactory, selectorFactory);
    } else {
      return makeProcessorInternal(
          factory -> new ColumnCapabilitiesImpl().setType(exprTypeHint)
                                                 .setHasMultipleValues(true)
                                                 .setDictionaryValuesUnique(false)
                                                 .setDictionaryValuesSorted(false),
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
   * @param inputCapabilitiesFn   function that returns capabilities of the column being processed. The type provided
   *                              by these capabilities will be used to determine what kind of selector to create. If
   *                              this function returns null, then processorFactory.defaultType() will be
   *                              used to construct a set of assumed capabilities.
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
      final Function<ColumnSelectorFactory, ColumnCapabilities> inputCapabilitiesFn,
      final Function<ColumnSelectorFactory, DimensionSelector> dimensionSelectorFn,
      final Function<ColumnSelectorFactory, ColumnValueSelector<?>> valueSelectorFunction,
      final ColumnProcessorFactory<T> processorFactory,
      final ColumnSelectorFactory selectorFactory
  )
  {
    final ColumnCapabilities capabilities = inputCapabilitiesFn.apply(selectorFactory);
    final ValueType effectiveType = capabilities != null ? capabilities.getType() : processorFactory.defaultType();

    switch (effectiveType) {
      case STRING:
        return processorFactory.makeDimensionProcessor(
            dimensionSelectorFn.apply(selectorFactory),
            mayBeMultiValue(capabilities)
        );
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

  /**
   * Returns true if a given set of capabilities might indicate an underlying multi-value column. Errs on the side
   * of returning true if unknown; i.e. if this returns false, there are _definitely not_ mul.
   */
  private static boolean mayBeMultiValue(@Nullable final ColumnCapabilities capabilities)
  {
    return capabilities == null || capabilities.hasMultipleValues().isMaybeTrue();
  }
}
