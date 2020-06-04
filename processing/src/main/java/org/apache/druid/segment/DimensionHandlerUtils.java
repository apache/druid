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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.dimension.ColumnSelectorStrategy;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class DimensionHandlerUtils
{

  // use these values to ensure that convertObjectToLong(), convertObjectToDouble() and convertObjectToFloat()
  // return the same boxed object when returning a constant zero.
  public static final Double ZERO_DOUBLE = 0.0d;
  public static final Float ZERO_FLOAT = 0.0f;
  public static final Long ZERO_LONG = 0L;

  public static final ColumnCapabilities DEFAULT_STRING_CAPABILITIES =
      new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                  .setDictionaryEncoded(false)
                                  .setDictionaryValuesUnique(false)
                                  .setDictionaryValuesSorted(false)
                                  .setHasBitmapIndexes(false);

  private DimensionHandlerUtils()
  {
  }

  public static DimensionHandler<?, ?, ?> getHandlerFromCapabilities(
      String dimensionName,
      @Nullable ColumnCapabilities capabilities,
      @Nullable MultiValueHandling multiValueHandling
  )
  {
    if (capabilities == null) {
      return new StringDimensionHandler(dimensionName, multiValueHandling, true);
    }

    multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;

    if (capabilities.getType() == ValueType.STRING) {
      if (!capabilities.isDictionaryEncoded()) {
        throw new IAE("String column must have dictionary encoding.");
      }
      return new StringDimensionHandler(dimensionName, multiValueHandling, capabilities.hasBitmapIndexes());
    }

    if (capabilities.getType() == ValueType.LONG) {
      return new LongDimensionHandler(dimensionName);
    }

    if (capabilities.getType() == ValueType.FLOAT) {
      return new FloatDimensionHandler(dimensionName);
    }

    if (capabilities.getType() == ValueType.DOUBLE) {
      return new DoubleDimensionHandler(dimensionName);
    }

    // Return a StringDimensionHandler by default (null columns will be treated as String typed)
    return new StringDimensionHandler(dimensionName, multiValueHandling, true);
  }

  public static List<ValueType> getValueTypesFromDimensionSpecs(List<DimensionSpec> dimSpecs)
  {
    List<ValueType> types = new ArrayList<>(dimSpecs.size());
    for (DimensionSpec dimSpec : dimSpecs) {
      types.add(dimSpec.getOutputType());
    }
    return types;
  }

  /**
   * Convenience function equivalent to calling
   * {@link #createColumnSelectorPluses(ColumnSelectorStrategyFactory, List, ColumnSelectorFactory)} with a singleton
   * list of dimensionSpecs and then retrieving the only element in the returned array.
   *
   * @param <Strategy> The strategy type created by the provided strategy factory.
   * @param strategyFactory               A factory provided by query engines that generates type-handling strategies
   * @param dimensionSpec                 column to generate a ColumnSelectorPlus object for
   * @param cursor                        Used to create value selectors for columns.
   *
   * @return A ColumnSelectorPlus object
   *
   * @see ColumnProcessors#makeProcessor which may replace this in the future
   */
  public static <Strategy extends ColumnSelectorStrategy> ColumnSelectorPlus<Strategy> createColumnSelectorPlus(
      ColumnSelectorStrategyFactory<Strategy> strategyFactory,
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory cursor
  )
  {
    return createColumnSelectorPluses(strategyFactory, ImmutableList.of(dimensionSpec), cursor)[0];
  }

  /**
   * Creates an array of ColumnSelectorPlus objects, selectors that handle type-specific operations within
   * query processing engines, using a strategy factory provided by the query engine. One ColumnSelectorPlus
   * will be created for each column specified in dimensionSpecs.
   *
   * The ColumnSelectorPlus provides access to a type strategy (e.g., how to group on a float column)
   * and a value selector for a single column.
   *
   * A caller should define a strategy factory that provides an interface for type-specific operations
   * in a query engine. See GroupByStrategyFactory for a reference.
   *
   * @param <Strategy>                    The strategy type created by the provided strategy factory.
   * @param strategyFactory               A factory provided by query engines that generates type-handling strategies
   * @param dimensionSpecs                The set of columns to generate ColumnSelectorPlus objects for
   * @param columnSelectorFactory         Used to create value selectors for columns.
   *
   * @return An array of ColumnSelectorPlus objects, in the order of the columns specified in dimensionSpecs
   *
   * @see ColumnProcessors#makeProcessor which may replace this in the future
   */
  public static <Strategy extends ColumnSelectorStrategy> ColumnSelectorPlus<Strategy>[] createColumnSelectorPluses(
      ColumnSelectorStrategyFactory<Strategy> strategyFactory,
      List<DimensionSpec> dimensionSpecs,
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    int dimCount = dimensionSpecs.size();
    @SuppressWarnings("unchecked")
    ColumnSelectorPlus<Strategy>[] dims = new ColumnSelectorPlus[dimCount];
    for (int i = 0; i < dimCount; i++) {
      final DimensionSpec dimSpec = dimensionSpecs.get(i);
      final String dimName = dimSpec.getDimension();
      final ColumnValueSelector<?> selector = getColumnValueSelectorFromDimensionSpec(
          dimSpec,
          columnSelectorFactory
      );
      Strategy strategy = makeStrategy(
          strategyFactory,
          dimSpec,
          columnSelectorFactory.getColumnCapabilities(dimSpec.getDimension()),
          selector
      );
      final ColumnSelectorPlus<Strategy> selectorPlus = new ColumnSelectorPlus<>(
          dimName,
          dimSpec.getOutputName(),
          strategy,
          selector
      );
      dims[i] = selectorPlus;
    }
    return dims;
  }

  private static ColumnValueSelector<?> getColumnValueSelectorFromDimensionSpec(
      DimensionSpec dimSpec,
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    String dimName = dimSpec.getDimension();
    ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(dimName);
    capabilities = getEffectiveCapabilities(dimSpec, capabilities);
    if (capabilities.getType() == ValueType.STRING) {
      return columnSelectorFactory.makeDimensionSelector(dimSpec);
    }
    return columnSelectorFactory.makeColumnValueSelector(dimSpec.getDimension());
  }

  /**
   * When determining the capabilities of a column during query processing, this function
   * adjusts the capabilities for columns that cannot be handled as-is to manageable defaults
   * (e.g., treating missing columns as empty String columns)
   */
  private static ColumnCapabilities getEffectiveCapabilities(
      DimensionSpec dimSpec,
      @Nullable ColumnCapabilities capabilities
  )
  {
    if (capabilities == null) {
      capabilities = DEFAULT_STRING_CAPABILITIES;
    }

    // Complex dimension type is not supported
    if (capabilities.getType() == ValueType.COMPLEX) {
      capabilities = DEFAULT_STRING_CAPABILITIES;
    }

    // Currently, all extractionFns output Strings, so the column will return String values via a
    // DimensionSelector if an extractionFn is present.
    if (dimSpec.getExtractionFn() != null) {
      ExtractionFn fn = dimSpec.getExtractionFn();
      capabilities = ColumnCapabilitiesImpl.copyOf(capabilities)
                                           .setType(ValueType.STRING)
                                           .setDictionaryValuesUnique(
                                               capabilities.isDictionaryEncoded() &&
                                               fn.getExtractionType() == ExtractionFn.ExtractionType.ONE_TO_ONE
                                           )
                                           .setDictionaryValuesSorted(
                                               capabilities.isDictionaryEncoded() && fn.preservesOrdering()
                                           );
    }

    // DimensionSpec's decorate only operates on DimensionSelectors, so if a spec mustDecorate(),
    // we need to wrap selectors on numeric columns with a string casting DimensionSelector.
    if (ValueType.isNumeric(capabilities.getType())) {
      if (dimSpec.mustDecorate()) {
        capabilities = DEFAULT_STRING_CAPABILITIES;
      }
    }

    return capabilities;
  }

  private static <Strategy extends ColumnSelectorStrategy> Strategy makeStrategy(
      ColumnSelectorStrategyFactory<Strategy> strategyFactory,
      DimensionSpec dimSpec,
      @Nullable ColumnCapabilities capabilities,
      ColumnValueSelector<?> selector
  )
  {
    capabilities = getEffectiveCapabilities(dimSpec, capabilities);
    return strategyFactory.makeColumnSelectorStrategy(capabilities, selector);
  }

  /**
   * Equivalent to calling makeVectorProcessor(DefaultDimensionSpec.of(column), strategyFactory, selectorFactory).
   *
   * @see #makeVectorProcessor(DimensionSpec, VectorColumnProcessorFactory, VectorColumnSelectorFactory)
   * @see ColumnProcessors#makeProcessor the non-vectorized version
   */
  public static <T> T makeVectorProcessor(
      final String column,
      final VectorColumnProcessorFactory<T> strategyFactory,
      final VectorColumnSelectorFactory selectorFactory
  )
  {
    return makeVectorProcessor(DefaultDimensionSpec.of(column), strategyFactory, selectorFactory);
  }

  /**
   * Creates "vector processors", which are objects that wrap a single vectorized input column and provide some
   * functionality on top of it. Used by things like query engines and filter matchers.
   *
   * Supports the basic types STRING, LONG, DOUBLE, and FLOAT.
   *
   * @param dimensionSpec   dimensionSpec for the input to the processor
   * @param strategyFactory object that encapsulates the knowledge about how to create processors
   * @param selectorFactory column selector factory used for creating the vector processor
   *
   * @see ColumnProcessors#makeProcessor the non-vectorized version
   */
  public static <T> T makeVectorProcessor(
      final DimensionSpec dimensionSpec,
      final VectorColumnProcessorFactory<T> strategyFactory,
      final VectorColumnSelectorFactory selectorFactory
  )
  {
    final ColumnCapabilities originalCapabilities =
        selectorFactory.getColumnCapabilities(dimensionSpec.getDimension());

    final ColumnCapabilities effectiveCapabilites = getEffectiveCapabilities(
        dimensionSpec,
        originalCapabilities
    );

    final ValueType type = effectiveCapabilites.getType();

    // vector selectors should never have null column capabilities, these signify a non-existent column, and complex
    // columns should never be treated as a multi-value column, so always use single value string processor
    final boolean forceSingleValue =
        originalCapabilities == null || ValueType.COMPLEX.equals(originalCapabilities.getType());

    if (type == ValueType.STRING) {
      if (!forceSingleValue && effectiveCapabilites.hasMultipleValues().isMaybeTrue()) {
        return strategyFactory.makeMultiValueDimensionProcessor(
            selectorFactory.makeMultiValueDimensionSelector(dimensionSpec)
        );
      } else {
        return strategyFactory.makeSingleValueDimensionProcessor(
            selectorFactory.makeSingleValueDimensionSelector(dimensionSpec)
        );
      }
    } else {
      Preconditions.checkState(
          dimensionSpec.getExtractionFn() == null && !dimensionSpec.mustDecorate(),
          "Uh oh, was about to try to make a value selector for type[%s] with a dimensionSpec of class[%s] that "
          + "requires decoration. Possible bug.",
          type,
          dimensionSpec.getClass().getName()
      );

      if (type == ValueType.LONG) {
        return strategyFactory.makeLongProcessor(
            selectorFactory.makeValueSelector(dimensionSpec.getDimension())
        );
      } else if (type == ValueType.FLOAT) {
        return strategyFactory.makeFloatProcessor(
            selectorFactory.makeValueSelector(dimensionSpec.getDimension())
        );
      } else if (type == ValueType.DOUBLE) {
        return strategyFactory.makeDoubleProcessor(
            selectorFactory.makeValueSelector(dimensionSpec.getDimension())
        );
      } else {
        throw new ISE("Unsupported type[%s]", effectiveCapabilites.getType());
      }
    }
  }

  @Nullable
  public static String convertObjectToString(@Nullable Object valObj)
  {
    if (valObj == null) {
      return null;
    }
    return valObj.toString();
  }

  @Nullable
  public static Long convertObjectToLong(@Nullable Object valObj)
  {
    return convertObjectToLong(valObj, false);
  }

  @Nullable
  public static Long convertObjectToLong(@Nullable Object valObj, boolean reportParseExceptions)
  {
    if (valObj == null) {
      return null;
    }

    if (valObj instanceof Long) {
      return (Long) valObj;
    } else if (valObj instanceof Number) {
      return ((Number) valObj).longValue();
    } else if (valObj instanceof String) {
      Long ret = DimensionHandlerUtils.getExactLongFromDecimalString((String) valObj);
      if (reportParseExceptions && ret == null) {
        throw new ParseException("could not convert value [%s] to long", valObj);
      }
      return ret;
    } else {
      throw new ParseException("Unknown type[%s]", valObj.getClass());
    }
  }

  @Nullable
  public static Float convertObjectToFloat(@Nullable Object valObj)
  {
    return convertObjectToFloat(valObj, false);
  }

  @Nullable
  public static Float convertObjectToFloat(@Nullable Object valObj, boolean reportParseExceptions)
  {
    if (valObj == null) {
      return null;
    }

    if (valObj instanceof Float) {
      return (Float) valObj;
    } else if (valObj instanceof Number) {
      return ((Number) valObj).floatValue();
    } else if (valObj instanceof String) {
      Float ret = Floats.tryParse((String) valObj);
      if (reportParseExceptions && ret == null) {
        throw new ParseException("could not convert value [%s] to float", valObj);
      }
      return ret;
    } else {
      throw new ParseException("Unknown type[%s]", valObj.getClass());
    }
  }

  @Nullable
  public static Comparable<?> convertObjectToType(
      @Nullable final Object obj,
      final ValueType type,
      final boolean reportParseExceptions
  )
  {
    Preconditions.checkNotNull(type, "type");

    switch (type) {
      case LONG:
        return convertObjectToLong(obj, reportParseExceptions);
      case FLOAT:
        return convertObjectToFloat(obj, reportParseExceptions);
      case DOUBLE:
        return convertObjectToDouble(obj, reportParseExceptions);
      case STRING:
        return convertObjectToString(obj);
      default:
        throw new IAE("Type[%s] is not supported for dimensions!", type);
    }
  }

  public static int compareObjectsAsType(
      @Nullable final Object lhs,
      @Nullable final Object rhs,
      final ValueType type
  )
  {
    //noinspection unchecked
    return Comparators.<Comparable>naturalNullsFirst().compare(
        convertObjectToType(lhs, type),
        convertObjectToType(rhs, type)
    );
  }

  @Nullable
  public static Comparable<?> convertObjectToType(@Nullable final Object obj, final ValueType type)
  {
    return convertObjectToType(obj, Preconditions.checkNotNull(type, "type"), false);
  }

  public static Function<Object, Comparable<?>> converterFromTypeToType(
      final ValueType fromType,
      final ValueType toType
  )
  {
    if (fromType == toType) {
      //noinspection unchecked
      return (Function) Function.identity();
    } else {
      return obj -> convertObjectToType(obj, toType);
    }
  }

  @Nullable
  public static Double convertObjectToDouble(@Nullable Object valObj)
  {
    return convertObjectToDouble(valObj, false);
  }

  @Nullable
  public static Double convertObjectToDouble(@Nullable Object valObj, boolean reportParseExceptions)
  {
    if (valObj == null) {
      return null;
    }

    if (valObj instanceof Double) {
      return (Double) valObj;
    } else if (valObj instanceof Number) {
      return ((Number) valObj).doubleValue();
    } else if (valObj instanceof String) {
      Double ret = Doubles.tryParse((String) valObj);
      if (reportParseExceptions && ret == null) {
        throw new ParseException("could not convert value [%s] to double", valObj);
      }
      return ret;
    } else {
      throw new ParseException("Unknown type[%s]", valObj.getClass());
    }
  }

  /**
   * Convert a string representing a decimal value to a long.
   *
   * If the decimal value is not an exact integral value (e.g. 42.0), or if the decimal value
   * is too large to be contained within a long, this function returns null.
   *
   * @param decimalStr string representing a decimal value
   *
   * @return long equivalent of decimalStr, returns null for non-integral decimals and integral decimal values outside
   * of the values representable by longs
   */
  @Nullable
  public static Long getExactLongFromDecimalString(String decimalStr)
  {
    final Long val = GuavaUtils.tryParseLong(decimalStr);
    if (val != null) {
      return val;
    }

    BigDecimal convertedBD;
    try {
      convertedBD = new BigDecimal(decimalStr);
    }
    catch (NumberFormatException nfe) {
      return null;
    }

    try {
      return convertedBD.longValueExact();
    }
    catch (ArithmeticException ae) {
      // indicates there was a non-integral part, or the BigDecimal was too big for a long
      return null;
    }
  }

  public static Double nullToZero(@Nullable Double number)
  {
    return number == null ? ZERO_DOUBLE : number;
  }

  public static Long nullToZero(@Nullable Long number)
  {
    return number == null ? ZERO_LONG : number;
  }

  public static Float nullToZero(@Nullable Float number)
  {
    return number == null ? ZERO_FLOAT : number;
  }
}
