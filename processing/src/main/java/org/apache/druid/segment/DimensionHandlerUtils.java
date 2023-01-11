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
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public final class DimensionHandlerUtils
{

  // use these values to ensure that convertObjectToLong(), convertObjectToDouble() and convertObjectToFloat()
  // return the same boxed object when returning a constant zero.
  public static final Double ZERO_DOUBLE = 0.0d;
  public static final Float ZERO_FLOAT = 0.0f;
  public static final Long ZERO_LONG = 0L;

  public static final ColumnCapabilities DEFAULT_STRING_CAPABILITIES =
      new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                  .setDictionaryEncoded(false)
                                  .setDictionaryValuesUnique(false)
                                  .setDictionaryValuesSorted(false)
                                  .setHasBitmapIndexes(false);

  public static final ConcurrentHashMap<String, DimensionHandlerProvider> DIMENSION_HANDLER_PROVIDERS = new ConcurrentHashMap<>();

  public static void registerDimensionHandlerProvider(String type, DimensionHandlerProvider provider)
  {
    DIMENSION_HANDLER_PROVIDERS.compute(type, (key, value) -> {
      if (value == null) {
        return provider;
      } else {
        if (!value.getClass().getName().equals(provider.getClass().getName())) {
          throw new ISE(
              "Incompatible dimensionHandlerProvider for type[%s] already exists. Expected [%s], found [%s].",
              key,
              value.getClass().getName(),
              provider.getClass().getName()
          );
        } else {
          return value;
        }
      }
    });
  }

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
      return new StringDimensionHandler(dimensionName, multiValueHandling, true, false);
    }

    multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;

    if (capabilities.is(ValueType.STRING)) {
      if (!capabilities.isDictionaryEncoded().isTrue()) {
        throw new IAE("String column must have dictionary encoding.");
      }
      return new StringDimensionHandler(
          dimensionName,
          multiValueHandling,
          capabilities.hasBitmapIndexes(),
          capabilities.hasSpatialIndexes()
      );
    }

    if (capabilities.is(ValueType.LONG)) {
      return new LongDimensionHandler(dimensionName);
    }

    if (capabilities.is(ValueType.FLOAT)) {
      return new FloatDimensionHandler(dimensionName);
    }

    if (capabilities.is(ValueType.DOUBLE)) {
      return new DoubleDimensionHandler(dimensionName);
    }

    if (capabilities.is(ValueType.COMPLEX) && capabilities.getComplexTypeName() != null) {
      DimensionHandlerProvider provider = DIMENSION_HANDLER_PROVIDERS.get(capabilities.getComplexTypeName());
      if (provider == null) {
        throw new ISE("Can't find DimensionHandlerProvider for typeName [%s]", capabilities.getComplexTypeName());
      }
      return provider.get(dimensionName);
    }

    // Return a StringDimensionHandler by default (null columns will be treated as String typed)
    return new StringDimensionHandler(dimensionName, multiValueHandling, true, false);
  }

  public static List<ColumnType> getValueTypesFromDimensionSpecs(List<DimensionSpec> dimSpecs)
  {
    List<ColumnType> types = new ArrayList<>(dimSpecs.size());
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
   * @param <Strategy>      The strategy type created by the provided strategy factory.
   * @param strategyFactory A factory provided by query engines that generates type-handling strategies
   * @param dimensionSpec   column to generate a ColumnSelectorPlus object for
   * @param cursor          Used to create value selectors for columns.
   * @return A ColumnSelectorPlus object
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
   * <p>
   * The ColumnSelectorPlus provides access to a type strategy (e.g., how to group on a float column)
   * and a value selector for a single column.
   * <p>
   * A caller should define a strategy factory that provides an interface for type-specific operations
   * in a query engine. See GroupByStrategyFactory for a reference.
   *
   * @param <Strategy>            The strategy type created by the provided strategy factory.
   * @param strategyFactory       A factory provided by query engines that generates type-handling strategies
   * @param dimensionSpecs        The set of columns to generate ColumnSelectorPlus objects for
   * @param columnSelectorFactory Used to create value selectors for columns.
   * @return An array of ColumnSelectorPlus objects, in the order of the columns specified in dimensionSpecs
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
    if (capabilities.is(ValueType.STRING)) {
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
    if (capabilities.is(ValueType.COMPLEX)) {
      capabilities = DEFAULT_STRING_CAPABILITIES;
    }

    // Currently, all extractionFns output Strings, so the column will return String values via a
    // DimensionSelector if an extractionFn is present.
    if (dimSpec.getExtractionFn() != null) {
      ExtractionFn fn = dimSpec.getExtractionFn();
      capabilities = ColumnCapabilitiesImpl.copyOf(capabilities)
                                           .setType(ColumnType.STRING)
                                           .setDictionaryValuesUnique(
                                               capabilities.isDictionaryEncoded().isTrue() &&
                                               fn.getExtractionType() == ExtractionFn.ExtractionType.ONE_TO_ONE
                                           )
                                           .setHasMultipleValues(
                                               capabilities.hasMultipleValues().isMaybeTrue() || capabilities.isArray()
                                           )
                                           .setDictionaryValuesSorted(
                                               capabilities.isDictionaryEncoded().isTrue() && fn.preservesOrdering()
                                           );
    }

    // DimensionSpec's decorate only operates on DimensionSelectors, so if a spec mustDecorate(),
    // we need to wrap selectors on numeric columns with a string casting DimensionSelector.
    if (capabilities.isNumeric()) {
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
        throw new ParseException((String) valObj, "could not convert value [%s] to long", valObj);
      }
      return ret;
    } else if (valObj instanceof List) {
      throw new ParseException(
          valObj.getClass().toString(),
          "Could not ingest value %s as long. A long column cannot have multiple values in the same row.",
          valObj
      );
    } else {
      throw new ParseException(
          valObj.getClass().toString(),
          "Could not convert value [%s] to long. Invalid type: [%s]",
          valObj,
          valObj.getClass()
      );
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
        throw new ParseException((String) valObj, "could not convert value [%s] to float", valObj);
      }
      return ret;
    } else if (valObj instanceof List) {
      throw new ParseException(
          valObj.getClass().toString(),
          "Could not ingest value %s as float. A float column cannot have multiple values in the same row.",
          valObj
      );
    } else {
      throw new ParseException(
          valObj.getClass().toString(),
          "Could not convert value [%s] to float. Invalid type: [%s]",
          valObj,
          valObj.getClass()
      );
    }
  }

  @Nullable
  public static Comparable<?> convertObjectToType(
      @Nullable final Object obj,
      final TypeSignature<ValueType> type,
      final boolean reportParseExceptions
  )
  {
    Preconditions.checkNotNull(type, "type");

    switch (type.getType()) {
      case LONG:
        return convertObjectToLong(obj, reportParseExceptions);
      case FLOAT:
        return convertObjectToFloat(obj, reportParseExceptions);
      case DOUBLE:
        return convertObjectToDouble(obj, reportParseExceptions);
      case STRING:
        return convertObjectToString(obj);
      case ARRAY:
        switch (type.getElementType().getType()) {
          case STRING:
            return convertToComparableStringArray(obj);
          case LONG:
            return convertToListWithObjectFunction(obj, DimensionHandlerUtils::convertObjectToLong);
          case FLOAT:
            return convertToListWithObjectFunction(obj, DimensionHandlerUtils::convertObjectToFloat);
          case DOUBLE:
            return convertToListWithObjectFunction(obj, DimensionHandlerUtils::convertObjectToDouble);
        }

      default:
        throw new IAE("Type[%s] is not supported for dimensions!", type);
    }
  }

  @Nullable
  public static ComparableList convertToList(Object obj, ValueType elementType)
  {
    switch (elementType) {
      case LONG:
        return convertToListWithObjectFunction(obj, DimensionHandlerUtils::convertObjectToLong);
      case FLOAT:
        return convertToListWithObjectFunction(obj, DimensionHandlerUtils::convertObjectToFloat);
      case DOUBLE:
        return convertToListWithObjectFunction(obj, DimensionHandlerUtils::convertObjectToDouble);
    }
    throw new ISE(
        "Unable to convert object of type[%s] to [%s]",
        obj.getClass().getName(),
        ComparableList.class.getName()
    );
  }


  private static <T> ComparableList convertToListWithObjectFunction(Object obj, Function<Object, T> convertFunction)
  {
    if (obj == null) {
      return null;
    }
    if (obj instanceof List) {
      return convertToComparableList((List) obj, convertFunction);
    }
    if (obj instanceof ComparableList) {
      return convertToComparableList(((ComparableList) obj).getDelegate(), convertFunction);
    }
    if (obj instanceof Object[]) {
      final List<T> delegateList = new ArrayList<>();
      for (Object eachObj : (Object[]) obj) {
        delegateList.add(convertFunction.apply(eachObj));
      }
      return new ComparableList(delegateList);
    }
    throw new ISE(
        "Unable to convert object of type[%s] to [%s]",
        obj.getClass().getName(),
        ComparableList.class.getName()
    );
  }

  @Nonnull
  private static <T> ComparableList convertToComparableList(List obj, Function<Object, T> convertFunction)
  {
    final List<T> delegateList = new ArrayList<>();
    for (Object eachObj : obj) {
      delegateList.add(convertFunction.apply(eachObj));
    }
    return new ComparableList(delegateList);
  }


  @Nullable
  public static ComparableStringArray convertToComparableStringArray(Object obj)
  {
    if (obj == null) {
      return null;
    }
    if (obj instanceof ComparableStringArray) {
      return (ComparableStringArray) obj;
    }
    // Jackson converts the serialized array into a list. Converting it back to a string array
    if (obj instanceof List) {
      String[] delegate = new String[((List) obj).size()];
      for (int i = 0; i < delegate.length; i++) {
        delegate[i] = convertObjectToString(((List) obj).get(i));
      }
      return ComparableStringArray.of(delegate);
    }
    if (obj instanceof Object[]) {
      Object[] objects = (Object[]) obj;
      String[] delegate = new String[objects.length];
      for (int i = 0; i < objects.length; i++) {
        delegate[i] = convertObjectToString(objects[i]);
      }
      return ComparableStringArray.of(delegate);
    }
    throw new ISE(
        "Unable to convert object of type[%s] to [%s]",
        obj.getClass().getName(),
        ComparableStringArray.class.getName()
    );
  }

  public static int compareObjectsAsType(
      @Nullable final Object lhs,
      @Nullable final Object rhs,
      final ColumnType type
  )
  {
    //noinspection unchecked
    return Comparators.<Comparable>naturalNullsFirst().compare(
        convertObjectToType(lhs, type),
        convertObjectToType(rhs, type)
    );
  }

  @Nullable
  public static Comparable<?> convertObjectToType(@Nullable final Object obj, final TypeSignature<ValueType> type)
  {
    return convertObjectToType(obj, Preconditions.checkNotNull(type, "type"), false);
  }

  public static Function<Object, Comparable<?>> converterFromTypeToType(
      final TypeSignature<ValueType> fromType,
      final TypeSignature<ValueType> toType
  )
  {
    if (Objects.equals(fromType, toType)) {
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
        throw new ParseException((String) valObj, "could not convert value [%s] to double", valObj);
      }
      return ret;
    } else if (valObj instanceof List) {
      throw new ParseException(
          valObj.getClass().toString(),
          "Could not ingest value %s as double. A double column cannot have multiple values in the same row.",
          valObj
      );
    } else {
      throw new ParseException(
          valObj.getClass().toString(),
          "Could not convert value [%s] to double. Invalid type: [%s]",
          valObj,
          valObj.getClass()
      );
    }
  }

  /**
   * Convert a string representing a decimal value to a long.
   * <p>
   * If the decimal value is not an exact integral value (e.g. 42.0), or if the decimal value
   * is too large to be contained within a long, this function returns null.
   *
   * @param decimalStr string representing a decimal value
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
