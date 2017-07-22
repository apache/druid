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

package io.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public final class DimensionHandlerUtils
{

  // use these values to ensure that convertObjectToLong(), convertObjectToDouble() and convertObjectToFloat()
  // return the same boxed object when returning a constant zero.
  public static final Double ZERO_DOUBLE = 0.0d;
  public static final Float ZERO_FLOAT = 0.0f;
  public static final Long ZERO_LONG = 0L;

  private DimensionHandlerUtils() {}

  public final static ColumnCapabilities DEFAULT_STRING_CAPABILITIES =
      new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                  .setDictionaryEncoded(true)
                                  .setHasBitmapIndexes(true);

  public static DimensionHandler getHandlerFromCapabilities(
      String dimensionName,
      ColumnCapabilities capabilities,
      MultiValueHandling multiValueHandling
  )
  {
    if (capabilities == null) {
      return new StringDimensionHandler(dimensionName, multiValueHandling);
    }

    multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;

    if (capabilities.getType() == ValueType.STRING) {
      if (!capabilities.isDictionaryEncoded() || !capabilities.hasBitmapIndexes()) {
        throw new IAE("String column must have dictionary encoding and bitmap index.");
      }
      return new StringDimensionHandler(dimensionName, multiValueHandling);
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
    return new StringDimensionHandler(dimensionName, multiValueHandling);
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
   * @param <ColumnSelectorStrategyClass> The strategy type created by the provided strategy factory.
   * @param strategyFactory               A factory provided by query engines that generates type-handling strategies
   * @param dimensionSpec                 column to generate a ColumnSelectorPlus object for
   * @param cursor                        Used to create value selectors for columns.
   *
   * @return A ColumnSelectorPlus object
   */
  public static <ColumnSelectorStrategyClass extends ColumnSelectorStrategy> ColumnSelectorPlus<ColumnSelectorStrategyClass> createColumnSelectorPlus(
      ColumnSelectorStrategyFactory<ColumnSelectorStrategyClass> strategyFactory,
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
   * @param <ColumnSelectorStrategyClass> The strategy type created by the provided strategy factory.
   * @param strategyFactory A factory provided by query engines that generates type-handling strategies
   * @param dimensionSpecs The set of columns to generate ColumnSelectorPlus objects for
   * @param cursor Used to create value selectors for columns.
   * @return An array of ColumnSelectorPlus objects, in the order of the columns specified in dimensionSpecs
   */
  public static <ColumnSelectorStrategyClass extends ColumnSelectorStrategy> ColumnSelectorPlus<ColumnSelectorStrategyClass>[] createColumnSelectorPluses(
      ColumnSelectorStrategyFactory<ColumnSelectorStrategyClass> strategyFactory,
      List<DimensionSpec> dimensionSpecs,
      ColumnSelectorFactory cursor
  )
  {
    int dimCount = dimensionSpecs.size();
    ColumnSelectorPlus<ColumnSelectorStrategyClass>[] dims = new ColumnSelectorPlus[dimCount];
    for (int i = 0; i < dimCount; i++) {
      final DimensionSpec dimSpec = dimensionSpecs.get(i);
      final String dimName = dimSpec.getDimension();
      final ColumnValueSelector selector = getColumnValueSelectorFromDimensionSpec(
          dimSpec,
          cursor
      );
      ColumnSelectorStrategyClass strategy = makeStrategy(
          strategyFactory,
          dimSpec,
          cursor.getColumnCapabilities(dimSpec.getDimension()),
          selector
      );
      final ColumnSelectorPlus<ColumnSelectorStrategyClass> selectorPlus = new ColumnSelectorPlus<>(
          dimName,
          dimSpec.getOutputName(),
          strategy,
          selector
      );
      dims[i] = selectorPlus;
    }
    return dims;
  }

  public static ColumnValueSelector getColumnValueSelectorFromDimensionSpec(
      DimensionSpec dimSpec,
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    String dimName = dimSpec.getDimension();
    ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(dimName);
    capabilities = getEffectiveCapabilities(dimSpec, capabilities);
    switch (capabilities.getType()) {
      case STRING:
        return columnSelectorFactory.makeDimensionSelector(dimSpec);
      case LONG:
        return columnSelectorFactory.makeLongColumnSelector(dimSpec.getDimension());
      case FLOAT:
        return columnSelectorFactory.makeFloatColumnSelector(dimSpec.getDimension());
      case DOUBLE:
        return columnSelectorFactory.makeDoubleColumnSelector(dimSpec.getDimension());
      default:
        return null;
    }
  }

  // When determining the capabilites of a column during query processing, this function
  // adjusts the capabilities for columns that cannot be handled as-is to manageable defaults
  // (e.g., treating missing columns as empty String columns)
  private static ColumnCapabilities getEffectiveCapabilities(
      DimensionSpec dimSpec,
      ColumnCapabilities capabilities
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
      capabilities = DEFAULT_STRING_CAPABILITIES;
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

  private static <ColumnSelectorStrategyClass extends ColumnSelectorStrategy> ColumnSelectorStrategyClass makeStrategy(
      ColumnSelectorStrategyFactory<ColumnSelectorStrategyClass> strategyFactory,
      DimensionSpec dimSpec,
      ColumnCapabilities capabilities,
      ColumnValueSelector selector
  )
  {
    capabilities = getEffectiveCapabilities(dimSpec, capabilities);
    return strategyFactory.makeColumnSelectorStrategy(capabilities, selector);
  }

  public static Long convertObjectToLong(@Nullable Object valObj)
  {
    if (valObj == null) {
      return ZERO_LONG;
    }

    if (valObj instanceof Long) {
      return (Long) valObj;
    } else if (valObj instanceof Number) {
      return ((Number) valObj).longValue();
    } else if (valObj instanceof String) {
      return DimensionHandlerUtils.getExactLongFromDecimalString((String) valObj);
    } else {
      throw new ParseException("Unknown type[%s]", valObj.getClass());
    }
  }

  public static Float convertObjectToFloat(@Nullable Object valObj)
  {
    if (valObj == null) {
      return ZERO_FLOAT;
    }

    if (valObj instanceof Float) {
      return (Float) valObj;
    } else if (valObj instanceof Number) {
      return ((Number) valObj).floatValue();
    } else if (valObj instanceof String) {
      return Floats.tryParse((String) valObj);
    } else {
      throw new ParseException("Unknown type[%s]", valObj.getClass());
    }
  }

  public static Double convertObjectToDouble(@Nullable Object valObj)
  {
    if (valObj == null) {
      return ZERO_DOUBLE;
    }

    if (valObj instanceof Double) {
      return (Double) valObj;
    } else if (valObj instanceof Number) {
      return ((Number) valObj).doubleValue();
    } else if (valObj instanceof String) {
      Double doubleValue = Doubles.tryParse((String) valObj);
      return  doubleValue == null ? ZERO_DOUBLE : doubleValue;
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
