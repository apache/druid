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
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.virtual.ExpressionSelectors;

import javax.annotation.Nullable;

/**
 * Creates "column processors", which are objects that wrap a single input column and provide some functionality on
 * top of it.
 *
 * @see DimensionHandlerUtils#createColumnSelectorPlus which this may eventually replace
 */
public class ColumnProcessors
{
  /**
   * Capabilites that are used when we return a nil selector for a nonexistent column.
   */
  public static final ColumnCapabilities NIL_COLUMN_CAPABILITIES =
      new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                  .setDictionaryEncoded(true)
                                  .setDictionaryValuesUnique(true)
                                  .setDictionaryValuesSorted(true)
                                  .setHasBitmapIndexes(false)
                                  .setHasMultipleValues(false);

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
        factory -> computeDimensionSpecCapabilities(
            dimensionSpec,
            factory.getColumnCapabilities(dimensionSpec.getDimension())
        ),
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
   * Make a processor for a particular named column.
   *
   * @param column           the column
   * @param processorFactory the processor factory
   * @param selectorFactory  the column selector factory
   * @param <T>              processor type
   */
  public static <T> T makeVectorProcessor(
      final String column,
      final VectorColumnProcessorFactory<T> processorFactory,
      final VectorColumnSelectorFactory selectorFactory
  )
  {
    return makeVectorProcessorInternal(
        factory -> factory.getColumnCapabilities(column),
        factory -> factory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(column)),
        factory -> factory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of(column)),
        factory -> factory.makeValueSelector(column),
        factory -> factory.makeObjectSelector(column),
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
  public static <T> T makeVectorProcessor(
      final DimensionSpec dimensionSpec,
      final VectorColumnProcessorFactory<T> processorFactory,
      final VectorColumnSelectorFactory selectorFactory
  )
  {
    return makeVectorProcessorInternal(
        factory -> computeDimensionSpecCapabilities(
            dimensionSpec,
            factory.getColumnCapabilities(dimensionSpec.getDimension())
        ),
        factory -> factory.makeSingleValueDimensionSelector(dimensionSpec),
        factory -> factory.makeMultiValueDimensionSelector(dimensionSpec),
        factory -> factory.makeValueSelector(dimensionSpec.getDimension()),
        factory -> factory.makeObjectSelector(dimensionSpec.getDimension()),
        processorFactory,
        selectorFactory
    );
  }

  /**
   * Returns the capabilities of selectors derived from a particular {@link DimensionSpec}.
   *
   * Will only return non-STRING types if the DimensionSpec passes through inputs unchanged. (i.e., it's a
   * {@link DefaultDimensionSpec}, or something that behaves like one.)
   *
   * @param dimensionSpec      The dimensionSpec.
   * @param columnCapabilities Capabilities of the column that the dimensionSpec is reading, i.e.
   *                           {@link DimensionSpec#getDimension()}.
   */
  @Nullable
  private static ColumnCapabilities computeDimensionSpecCapabilities(
      final DimensionSpec dimensionSpec,
      @Nullable final ColumnCapabilities columnCapabilities
  )
  {
    if (dimensionSpec.mustDecorate()) {
      // Decorating DimensionSpecs could do anything. We can't pass along any useful info other than the type.
      return new ColumnCapabilitiesImpl().setType(ValueType.STRING);
    } else if (dimensionSpec.getExtractionFn() != null) {
      // DimensionSpec is applying an extractionFn but *not* decorating. We have some insight into how the
      // extractionFn will behave, so let's use it.

      return new ColumnCapabilitiesImpl()
          .setType(ValueType.STRING)
          .setDictionaryValuesSorted(dimensionSpec.getExtractionFn().preservesOrdering())
          .setDictionaryValuesUnique(dimensionSpec.getExtractionFn().getExtractionType()
                                     == ExtractionFn.ExtractionType.ONE_TO_ONE)
          .setHasMultipleValues(dimensionSpec.mustDecorate() || mayBeMultiValue(columnCapabilities));
    } else {
      // No transformation. Pass through underlying types.
      return columnCapabilities;
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
   * Creates "column processors", which are objects that wrap a single input column and provide some
   * functionality on top of it.
   *
   * @param inputCapabilitiesFn            function that returns capabilities of the column being processed. The type provided
   *                                       by these capabilities will be used to determine what kind of selector to create. If
   *                                       this function returns null, then it is assumed that the column does not exist.
   *                                       Note: this is different behavior from the non-vectorized version.
   * @param singleValueDimensionSelectorFn function that creates a singly-valued dimension selector for the column being
   *                                       processed. Will be called if the column is singly-valued string.
   * @param multiValueDimensionSelectorFn  function that creates a multi-valued dimension selector for the column being
   *                                       processed. Will be called if the column is multi-valued string.
   * @param valueSelectorFn                function that creates a value selector for the column being processed. Will be
   *                                       called if the column type is long, float, or double.
   * @param objectSelectorFn               function that creates an object selector for the column being processed. Will
   *                                       be called if the column type is complex.
   * @param processorFactory               object that encapsulates the knowledge about how to create processors
   * @param selectorFactory                column selector factory used for creating the vector processor
   */
  private static <T> T makeVectorProcessorInternal(
      final Function<VectorColumnSelectorFactory, ColumnCapabilities> inputCapabilitiesFn,
      final Function<VectorColumnSelectorFactory, SingleValueDimensionVectorSelector> singleValueDimensionSelectorFn,
      final Function<VectorColumnSelectorFactory, MultiValueDimensionVectorSelector> multiValueDimensionSelectorFn,
      final Function<VectorColumnSelectorFactory, VectorValueSelector> valueSelectorFn,
      final Function<VectorColumnSelectorFactory, VectorObjectSelector> objectSelectorFn,
      final VectorColumnProcessorFactory<T> processorFactory,
      final VectorColumnSelectorFactory selectorFactory
  )
  {
    final ColumnCapabilities capabilities = inputCapabilitiesFn.apply(selectorFactory);

    if (capabilities == null) {
      // Column does not exist.
      return processorFactory.makeSingleValueDimensionProcessor(
          NIL_COLUMN_CAPABILITIES,
          NilVectorSelector.create(selectorFactory.getReadableVectorInspector())
      );
    }

    switch (capabilities.getType()) {
      case STRING:
        if (mayBeMultiValue(capabilities)) {
          return processorFactory.makeMultiValueDimensionProcessor(
              capabilities,
              multiValueDimensionSelectorFn.apply(selectorFactory)
          );
        } else {
          return processorFactory.makeSingleValueDimensionProcessor(
              capabilities,
              singleValueDimensionSelectorFn.apply(selectorFactory)
          );
        }
      case LONG:
        return processorFactory.makeLongProcessor(capabilities, valueSelectorFn.apply(selectorFactory));
      case FLOAT:
        return processorFactory.makeFloatProcessor(capabilities, valueSelectorFn.apply(selectorFactory));
      case DOUBLE:
        return processorFactory.makeDoubleProcessor(capabilities, valueSelectorFn.apply(selectorFactory));
      case COMPLEX:
        return processorFactory.makeObjectProcessor(capabilities, objectSelectorFn.apply(selectorFactory));
      default:
        throw new ISE("Unsupported type[%s]", capabilities.getType());
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
