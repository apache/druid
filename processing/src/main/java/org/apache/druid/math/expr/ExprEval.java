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

package org.apache.druid.math.expr;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.segment.nested.StructuredData;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Generic result holder for evaluated {@link Expr} containing the value and {@link ExprType} of the value to allow
 */
public abstract class ExprEval<T>
{
  /**
   * Deserialize an expression stored in a bytebuffer, e.g. for an agg.
   *
   * This method is not thread-safe with respect to the provided {@link ByteBuffer}, because the position of the
   * buffer may be changed transiently during execution of this method. However, it will be restored to its original
   * position prior to the method completing. Therefore, if the provided buffer is being used by a single thread, then
   * this method does not change the position of the buffer.
   *
   * The {@code canRetainBufferReference} parameter determines
   *
   * @param buffer                   source buffer
   * @param offset                   position to start reading from
   * @param maxSize                  maximum number of bytes from "offset" that may be required. This is used as advice,
   *                                 but is not strictly enforced in all cases. It is possible that type strategies may
   *                                 attempt reads past this limit.
   * @param type                     data type to read
   * @param canRetainBufferReference whether the returned {@link ExprEval} may retain a reference to the provided
   *                                 {@link ByteBuffer}. Certain types are deserialized more efficiently if allowed
   *                                 to retain references to the provided buffer.
   */
  public static ExprEval deserialize(
      final ByteBuffer buffer,
      final int offset,
      final int maxSize,
      final ExpressionType type,
      final boolean canRetainBufferReference
  )
  {
    switch (type.getType()) {
      case LONG:
        if (TypeStrategies.isNullableNull(buffer, offset)) {
          return ofLong(null);
        }
        return of(TypeStrategies.readNotNullNullableLong(buffer, offset));
      case DOUBLE:
        if (TypeStrategies.isNullableNull(buffer, offset)) {
          return ofDouble(null);
        }
        return of(TypeStrategies.readNotNullNullableDouble(buffer, offset));
      default:
        final NullableTypeStrategy<Object> strategy = type.getNullableStrategy();

        if (!canRetainBufferReference && strategy.readRetainsBufferReference()) {
          final ByteBuffer dataCopyBuffer = ByteBuffer.allocate(maxSize);
          final ByteBuffer mutationBuffer = buffer.duplicate();
          mutationBuffer.limit(offset + maxSize);
          mutationBuffer.position(offset);
          dataCopyBuffer.put(mutationBuffer);
          dataCopyBuffer.rewind();
          return ofType(type, strategy.read(dataCopyBuffer, 0));
        } else {
          return ofType(type, strategy.read(buffer, offset));
        }
    }
  }

  /**
   * Write an expression result to a bytebuffer, throwing an {@link ISE} if the data exceeds a maximum size. Primitive
   * numeric types are not validated to be lower than max size, so it is expected to be at least 10 bytes. Callers
   * of this method should enforce this themselves (instead of doing it here, which might be done every row)
   *
   * This should be refactored to be consolidated with some of the standard type handling of aggregators probably
   */
  public static void serialize(ByteBuffer buffer, int position, ExpressionType type, ExprEval<?> eval, int maxSizeBytes)
  {
    int offset = position;
    switch (type.getType()) {
      case LONG:
        if (eval.value() == null) {
          TypeStrategies.writeNull(buffer, offset);
        } else {
          TypeStrategies.writeNotNullNullableLong(buffer, offset, eval.asLong());
        }
        break;
      case DOUBLE:
        if (eval.value() == null) {
          TypeStrategies.writeNull(buffer, offset);
        } else {
          TypeStrategies.writeNotNullNullableDouble(buffer, offset, eval.asDouble());
        }
        break;
      default:
        final NullableTypeStrategy strategy = type.getNullableStrategy();
        // if the types don't match, cast it so things don't get weird
        if (type.equals(eval.type())) {
          eval = eval.castTo(type);
        }
        int written = strategy.write(buffer, offset, eval.value(), maxSizeBytes);
        if (written < 0) {
          throw new ISE(
              "Unable to serialize [%s], max size bytes is [%s], but need at least [%s] bytes to write entire value",
              type.asTypeString(),
              maxSizeBytes,
              maxSizeBytes - written
          );
        }
    }
  }

  /**
   * Converts a List to an appropriate array type, optionally doing some conversion to make multi-valued strings
   * consistent across selector types, which are not consistent in treatment of null, [], and [null].
   *
   * If homogenizeMultiValueStrings is true, null and [] will be converted to [null], otherwise they will retain
   */
  @Nullable
  public static NonnullPair<ExpressionType, Object[]> coerceListToArray(@Nullable List<?> val, boolean homogenizeMultiValueStrings)
  {
    // if value is not null and has at least 1 element, conversion is unambigous regardless of the selector
    if (val != null && val.size() > 0) {
      Class<?> coercedType = null;

      for (Object elem : val) {
        if (elem != null) {
          coercedType = convertType(coercedType, elem.getClass());
        }
      }

      if (coercedType == Long.class || coercedType == Integer.class) {
        Object[] array = new Object[val.size()];
        int i = 0;
        for (Object o : val) {
          array[i++] = o != null ? ExprEval.ofType(ExpressionType.LONG, o).value() : null;
        }
        return new NonnullPair<>(ExpressionType.LONG_ARRAY, array);
      } else if (coercedType == Float.class || coercedType == Double.class) {
        Object[] array = new Object[val.size()];
        int i = 0;
        for (Object o : val) {
          array[i++] = o != null ? ExprEval.ofType(ExpressionType.DOUBLE, o).value() : null;
        }
        return new NonnullPair<>(ExpressionType.DOUBLE_ARRAY, array);
      } else if (coercedType == Object.class) {
        // object, fall back to "best effort"
        ExprEval<?>[] evals = new ExprEval[val.size()];
        Object[] array = new Object[val.size()];
        int i = 0;
        ExpressionType elementType = null;
        for (Object o : val) {
          if (o != null) {
            ExprEval<?> eval = ExprEval.bestEffortOf(o);
            elementType = ExpressionTypeConversion.leastRestrictiveType(elementType, eval.type());
            evals[i++] = eval;
          } else {
            evals[i++] = null;
          }
        }
        i = 0;
        for (ExprEval<?> eval : evals) {
          if (eval != null) {
            array[i++] = eval.castTo(elementType).value();
          } else {
            array[i++] = ExprEval.ofType(elementType, null).value();
          }
        }
        ExpressionType arrayType = elementType == null
                                   ? ExpressionType.STRING_ARRAY
                                   : ExpressionTypeFactory.getInstance().ofArray(elementType);
        return new NonnullPair<>(arrayType, array);
      }
      // default to string
      Object[] array = new Object[val.size()];
      int i = 0;
      for (Object o : val) {
        array[i++] = o != null ? ExprEval.ofType(ExpressionType.STRING, o).value() : null;
      }
      return new NonnullPair<>(ExpressionType.STRING_ARRAY, array);
    }
    if (homogenizeMultiValueStrings) {
      return new NonnullPair<>(ExpressionType.STRING_ARRAY, new Object[]{null});
    } else {
      if (val != null) {
        return new NonnullPair<>(ExpressionType.STRING_ARRAY, new Object[0]);
      }
      return null;
    }
  }

  /**
   * Find the common type to use between 2 types, useful for choosing the appropriate type for an array given a set
   * of objects with unknown type, following rules similar to Java, our own native Expr, and SQL implicit type
   * conversions. This is used to assist in preparing native java objects for {@link Expr.ObjectBinding} which will
   * later be wrapped in {@link ExprEval} when evaluating {@link IdentifierExpr}.
   *
   * If any type is string, then the result will be string because everything can be converted to a string, but a string
   * cannot be converted to everything.
   *
   * For numbers, integer is the most restrictive type, only chosen if both types are integers. Longs win over integers,
   * floats over longs and integers, and doubles win over everything.
   */
  private static Class convertType(@Nullable Class existing, Class next)
  {
    if (existing != null && existing.equals(Object.class)) {
      return existing;
    }
    if (Number.class.isAssignableFrom(next) || next == String.class || next == Boolean.class) {
      // coerce booleans
      if (next == Boolean.class) {
        if (ExpressionProcessing.useStrictBooleans()) {
          next = Long.class;
        } else {
          next = String.class;
        }
      }
      if (existing == null) {
        return next;
      }
      // string wins everything
      if (existing == String.class) {
        return existing;
      }
      if (next == String.class) {
        return next;
      }

      // all numbers win over Integer
      if (existing == Integer.class) {
        return next;
      }
      if (existing == Float.class) {
        // doubles win over floats
        if (next == Double.class) {
          return next;
        }
        return existing;
      }
      if (existing == Long.class) {
        if (next == Integer.class) {
          // long beats int
          return existing;
        }
        // double and float win over longs
        return next;
      }
      // otherwise double
      return Double.class;
    }
    // its complicated, call it object
    return Object.class;
  }

  public static ExprEval of(long longValue)
  {
    return new LongExprEval(longValue);
  }

  public static ExprEval of(double doubleValue)
  {
    return new DoubleExprEval(doubleValue);
  }

  public static ExprEval of(@Nullable String stringValue)
  {
    if (stringValue == null) {
      return StringExprEval.OF_NULL;
    }
    return new StringExprEval(stringValue);
  }

  public static ExprEval ofLong(@Nullable Number longValue)
  {
    if (longValue == null) {
      return LongExprEval.OF_NULL;
    }
    return new LongExprEval(longValue);
  }

  public static ExprEval ofDouble(@Nullable Number doubleValue)
  {
    if (doubleValue == null) {
      return DoubleExprEval.OF_NULL;
    }
    return new DoubleExprEval(doubleValue);
  }

  public static ExprEval ofLongArray(@Nullable Object[] longValue)
  {
    if (longValue == null) {
      return ArrayExprEval.OF_NULL_LONG;
    }
    return new ArrayExprEval(ExpressionType.LONG_ARRAY, longValue);
  }

  public static ExprEval ofDoubleArray(@Nullable Object[] doubleValue)
  {
    if (doubleValue == null) {
      return ArrayExprEval.OF_NULL_DOUBLE;
    }
    return new ArrayExprEval(ExpressionType.DOUBLE_ARRAY, doubleValue);
  }

  public static ExprEval ofStringArray(@Nullable Object[] stringValue)
  {
    if (stringValue == null) {
      return ArrayExprEval.OF_NULL_STRING;
    }
    return new ArrayExprEval(ExpressionType.STRING_ARRAY, stringValue);
  }


  public static ExprEval ofArray(ExpressionType outputType, @Nullable Object[] value)
  {
    Preconditions.checkArgument(outputType.isArray(), "Output type %s is not an array", outputType);
    return new ArrayExprEval(outputType, value);
  }

  /**
   * Convert a boolean back into native expression type
   *
   * Do not use this method unless {@link ExpressionProcessing#useStrictBooleans()} is set to false.
   * {@link ExpressionType#LONG} is the Druid boolean unless this mode is enabled, so use {@link #ofLongBoolean}
   * instead.
   */
  @Deprecated
  public static ExprEval ofBoolean(boolean value, ExpressionType type)
  {
    switch (type.getType()) {
      case DOUBLE:
        return ExprEval.of(Evals.asDouble(value));
      case LONG:
        return ofLongBoolean(value);
      case STRING:
        return ExprEval.of(String.valueOf(value));
      default:
        throw new Types.InvalidCastBooleanException(type);
    }
  }

  /**
   * Convert a boolean into a long expression type
   */
  public static ExprEval ofLongBoolean(boolean value)
  {
    return value ? LongExprEval.TRUE : LongExprEval.FALSE;
  }

  public static ExprEval ofComplex(ExpressionType outputType, @Nullable Object value)
  {
    if (ExpressionType.NESTED_DATA.equals(outputType)) {
      return new NestedDataExprEval(value);
    }
    return new ComplexExprEval(outputType, value);
  }

  public static ExprEval bestEffortArray(@Nullable List<?> theList)
  {
    // do not convert empty lists to arrays with a single null element here, because that should have been done
    // by the selectors preparing their ObjectBindings if necessary. If we get to this point it was legitimately
    // empty
    NonnullPair<ExpressionType, Object[]> coerced = coerceListToArray(theList, false);
    if (coerced == null) {
      return bestEffortOf(null);
    }
    return ofArray(coerced.lhs, coerced.rhs);
  }

  /**
   * Examine java type to find most appropriate expression type
   */
  public static ExprEval bestEffortOf(@Nullable Object val)
  {
    if (val == null) {
      return new StringExprEval(null);
    }
    if (val instanceof ExprEval) {
      return (ExprEval) val;
    }
    if (val instanceof String) {
      return new StringExprEval((String) val);
    }
    if (val instanceof Number) {
      if (val instanceof Float || val instanceof Double) {
        return new DoubleExprEval((Number) val);
      }
      return new LongExprEval((Number) val);
    }
    if (val instanceof Boolean) {
      if (ExpressionProcessing.useStrictBooleans()) {
        return ofLongBoolean((Boolean) val);
      }
      return new StringExprEval(String.valueOf(val));
    }
    if (val instanceof Long[]) {
      final Long[] inputArray = (Long[]) val;
      final Object[] array = new Object[inputArray.length];
      for (int i = 0; i < inputArray.length; i++) {
        array[i] = inputArray[i];
      }
      return new ArrayExprEval(ExpressionType.LONG_ARRAY, array);
    }
    if (val instanceof long[]) {
      final long[] longArray = (long[]) val;
      final Object[] array = new Object[longArray.length];
      for (int i = 0; i < longArray.length; i++) {
        array[i] = longArray[i];
      }
      return new ArrayExprEval(ExpressionType.LONG_ARRAY, array);
    }
    if (val instanceof Integer[]) {
      final Integer[] inputArray = (Integer[]) val;
      final Object[] array = new Object[inputArray.length];
      for (int i = 0; i < inputArray.length; i++) {
        array[i] = inputArray[i] != null ? inputArray[i].longValue() : null;
      }
      return new ArrayExprEval(ExpressionType.LONG_ARRAY, array);
    }
    if (val instanceof int[]) {
      final int[] longArray = (int[]) val;
      final Object[] array = new Object[longArray.length];
      for (int i = 0; i < longArray.length; i++) {
        array[i] = (long) longArray[i];
      }
      return new ArrayExprEval(ExpressionType.LONG_ARRAY, array);
    }
    if (val instanceof Double[]) {
      final Double[] inputArray = (Double[]) val;
      final Object[] array = new Object[inputArray.length];
      for (int i = 0; i < inputArray.length; i++) {
        array[i] = inputArray[i] != null ? inputArray[i] : null;
      }
      return new ArrayExprEval(ExpressionType.DOUBLE_ARRAY, array);
    }
    if (val instanceof double[]) {
      final double[] inputArray = (double[]) val;
      final Object[] array = new Object[inputArray.length];
      for (int i = 0; i < inputArray.length; i++) {
        array[i] = inputArray[i];
      }
      return new ArrayExprEval(ExpressionType.DOUBLE_ARRAY, array);
    }
    if (val instanceof Float[]) {
      final Float[] inputArray = (Float[]) val;
      final Object[] array = new Object[inputArray.length];
      for (int i = 0; i < inputArray.length; i++) {
        array[i] = inputArray[i] != null ? inputArray[i].doubleValue() : null;
      }
      return new ArrayExprEval(ExpressionType.DOUBLE_ARRAY, array);
    }
    if (val instanceof float[]) {
      final float[] inputArray = (float[]) val;
      final Object[] array = new Object[inputArray.length];
      for (int i = 0; i < inputArray.length; i++) {
        array[i] = (double) inputArray[i];
      }
      return new ArrayExprEval(ExpressionType.DOUBLE_ARRAY, array);
    }
    if (val instanceof String[]) {
      final String[] inputArray = (String[]) val;
      final Object[] array = new Object[inputArray.length];
      for (int i = 0; i < inputArray.length; i++) {
        array[i] = inputArray[i];
      }
      return new ArrayExprEval(ExpressionType.STRING_ARRAY, array);
    }

    if (val instanceof List || val instanceof Object[]) {
      final List<?> theList = val instanceof List ? ((List<?>) val) : Arrays.asList((Object[]) val);
      return bestEffortArray(theList);
    }
    // handle leaky group by array types
    if (val instanceof ComparableStringArray) {
      return new ArrayExprEval(ExpressionType.STRING_ARRAY, ((ComparableStringArray) val).getDelegate());
    }
    if (val instanceof ComparableList) {
      return bestEffortArray(((ComparableList) val).getDelegate());
    }

    // in 'best effort' mode, we couldn't possibly use byte[] as a complex or anything else useful without type
    // knowledge, so lets turn it into a base64 encoded string so at least something downstream can use it by decoding
    // back into bytes
    if (val instanceof byte[]) {
      return new StringExprEval(StringUtils.encodeBase64String((byte[]) val));
    }


    if (val instanceof Map) {
      return ofComplex(ExpressionType.NESTED_DATA, val);
    }

    // is this cool?
    return ofComplex(ExpressionType.UNKNOWN_COMPLEX, val);
  }

  public static ExprEval ofType(@Nullable ExpressionType type, @Nullable Object value)
  {
    if (type == null) {
      return bestEffortOf(value);
    }
    switch (type.getType()) {
      case STRING:
        // not all who claim to be "STRING" are always a String, prepare ourselves...
        if (value instanceof String[]) {
          final String[] inputArray = (String[]) value;
          final Object[] array = new Object[inputArray.length];
          for (int i = 0; i < inputArray.length; i++) {
            array[i] = inputArray[i];
          }
          return new ArrayExprEval(ExpressionType.STRING_ARRAY, array);
        }
        if (value instanceof Object[]) {
          return bestEffortOf(value);
        }
        if (value instanceof List) {
          return bestEffortOf(value);
        }
        if (value instanceof byte[]) {
          return new StringExprEval(StringUtils.encodeBase64String((byte[]) value));
        }
        return of(Evals.asString(value));
      case LONG:
        if (value instanceof Number) {
          return ofLong((Number) value);
        }
        if (value instanceof Boolean) {
          return ofLongBoolean((Boolean) value);
        }
        if (value instanceof String) {
          return ofLong(ExprEval.computeNumber((String) value));
        }
        return ofLong(null);
      case DOUBLE:
        if (value instanceof Number) {
          return ofDouble((Number) value);
        }
        if (value instanceof Boolean) {
          return ofDouble(Evals.asDouble((Boolean) value));
        }
        if (value instanceof String) {
          return ofDouble(ExprEval.computeNumber((String) value));
        }
        return ofDouble(null);
      case COMPLEX:
        if (ExpressionType.NESTED_DATA.equals(type)) {
          return ofComplex(type, StructuredData.unwrap(value));
        }
        byte[] bytes = null;
        if (value instanceof String) {
          try {
            bytes = StringUtils.decodeBase64String((String) value);
          }
          catch (IllegalArgumentException ignored) {
          }
        } else if (value instanceof byte[]) {
          bytes = (byte[]) value;
        }

        if (bytes != null) {
          TypeStrategy<?> strategy = type.getStrategy();
          ByteBuffer bb = ByteBuffer.wrap(bytes);
          return ofComplex(type, strategy.read(bb));
        }

        return ofComplex(type, value);
      case ARRAY:
        ExpressionType elementType = (ExpressionType) type.getElementType();
        if (value == null) {
          return ofArray(type, null);
        }
        if (value instanceof List) {
          List<?> theList = (List<?>) value;
          Object[] array = new Object[theList.size()];
          int i = 0;
          for (Object o : theList) {
            array[i++] = ExprEval.ofType(elementType, o).value();
          }
          return ofArray(type, array);
        }

        if (value instanceof Object[]) {
          Object[] inputArray = (Object[]) value;
          Object[] array = new Object[inputArray.length];
          int i = 0;
          for (Object o : inputArray) {
            array[i++] = ExprEval.ofType(elementType, o).value();
          }
          return ofArray(type, array);
        }
        // in a better world, we might get an object that matches the type signature for arrays and could do a switch
        // statement here, but this is not that world yet, and things that are array typed might also be non-arrays,
        // e.g. we might get a String instead of String[], so just fallback to bestEffortOf
        return bestEffortOf(value).castTo(type);
    }
    throw new IAE("Cannot create type [%s]", type);
  }

  @Nullable
  public static Number computeNumber(@Nullable String value)
  {
    if (value == null) {
      return null;
    }
    if (Evals.asBoolean(value)) {
      return 1.0;
    }
    if (value.equalsIgnoreCase("false")) {
      return 0.0;
    }
    Number rv;
    Long v = GuavaUtils.tryParseLong(value);
    // Do NOT use ternary operator here, because it makes Java to convert Long to Double
    if (v != null) {
      rv = v;
    } else {
      rv = Doubles.tryParse(value);
    }
    return rv;
  }

  /**
   * Cast an {@link ExprEval} to some {@link ExpressionType} that the value will be compared with. If the value is not
   * appropriate to use for comparison after casting, this method returns null. For example, the
   * {@link ExpressionType#DOUBLE} value 1.1 when cast to {@link ExpressionType#LONG} becomes 1L, which is no longer
   * appropriate to use for value equality comparisons, while 1.0 is valid.
   */
  @Nullable
  public static ExprEval<?> castForEqualityComparison(ExprEval<?> valueToCompare, ExpressionType typeToCompareWith)
  {
    if (valueToCompare.isArray() && !typeToCompareWith.isArray()) {
      final Object[] array = valueToCompare.asArray();
      // cannot cast array to scalar if array length is greater than 1
      if (array != null && array.length != 1) {
        return null;
      }
    }
    ExprEval<?> cast = valueToCompare.castTo(typeToCompareWith);
    if (ExpressionType.LONG.equals(typeToCompareWith) && valueToCompare.asDouble() != cast.asDouble()) {
      // make sure the DOUBLE value when cast to LONG is the same before and after the cast
      // this lets us match 1.0 to 1, but not 1.1
      return null;
    } else if (ExpressionType.LONG_ARRAY.equals(typeToCompareWith)) {
      // if comparison array is double typed, make sure the values are the same when cast to long
      // this lets us match [1.0, 2.0, 3.0] to [1, 2, 3], but not [1.1, 2.2, 3.3]
      final ExprEval<?> doubleCast = valueToCompare.castTo(ExpressionType.DOUBLE_ARRAY);
      final ExprEval<?> castDoubleCast = cast.castTo(ExpressionType.DOUBLE_ARRAY);
      if (ExpressionType.DOUBLE_ARRAY.getStrategy().compare(doubleCast.value(), castDoubleCast.value()) != 0) {
        return null;
      }
    }

    // did value become null during cast but was not initially null?
    if (valueToCompare.value() != null && cast.value() == null) {
      return null;
    }
    return cast;
  }

  // Cached String values
  private boolean stringValueCached = false;
  @Nullable
  private String stringValue;

  @Nullable
  final T value;

  private ExprEval(@Nullable T value)
  {
    this.value = value;
  }

  public abstract ExpressionType type();

  public ExpressionType elementType()
  {
    return type().isArray() ? (ExpressionType) type().getElementType() : type();
  }

  public ExpressionType asArrayType()
  {
    return type().isArray() ? type() : ExpressionTypeFactory.getInstance().ofArray(type());
  }

  @Nullable
  public T value()
  {
    return value;
  }

  @Nullable
  public T valueOrDefault()
  {
    return value;
  }

  void cacheStringValue(@Nullable String value)
  {
    stringValue = value;
    stringValueCached = true;
  }

  @Nullable
  String getCachedStringValue()
  {
    return stringValue;
  }

  boolean isStringValueCached()
  {
    return stringValueCached;
  }

  @Nullable
  public String asString()
  {
    if (!stringValueCached) {
      stringValue = Evals.asString(value);
      stringValueCached = true;
    }

    return stringValue;
  }

  /**
   * The method returns true if numeric primitive value for this {@link ExprEval} is null, otherwise false.
   *
   * If this method returns false, then the values returned by {@link #asLong()}, {@link #asDouble()},
   * and {@link #asInt()} are "valid", since this method is primarily used during {@link Expr} evaluation to decide
   * if primitive numbers can be fetched to use.
   *
   * If a type cannot sanely convert into a primitive numeric value, then this method should always return true so that
   * these primitive numeric getters are not called, since returning false is assumed to mean these values are valid.
   *
   * Note that all types must still return values for {@link #asInt()}, {@link #asLong()}}, and {@link #asDouble()},
   * since this can still happen if {@link NullHandling#sqlCompatible()} is false, but it should be assumed that this
   * can only happen in that mode and 0s are typical and expected for values that would otherwise be null.
   */
  public abstract boolean isNumericNull();

  public boolean isArray()
  {
    return false;
  }

  /**
   * Get the primitive integer value. Callers should check {@link #isNumericNull()} prior to calling this method,
   * otherwise it may improperly return placeholder a value (typically zero, which is expected if
   * {@link NullHandling#sqlCompatible()} is false)
   */
  public abstract int asInt();

  /**
   * Get the primitive long value. Callers should check {@link #isNumericNull()} prior to calling this method,
   * otherwise it may improperly return a placeholder value (typically zero, which is expected if
   * {@link NullHandling#sqlCompatible()} is false)
   */
  public abstract long asLong();

  /**
   * Get the primitive double value. Callers should check {@link #isNumericNull()} prior to calling this method,
   * otherwise it may improperly return a placeholder value (typically zero, which is expected if
   * {@link NullHandling#sqlCompatible()} is false)
   */
  public abstract double asDouble();

  public abstract boolean asBoolean();

  @Nullable
  public abstract Object[] asArray();

  public abstract ExprEval castTo(ExpressionType castTo);

  public abstract Expr toExpr();

  private abstract static class NumericExprEval extends ExprEval<Number>
  {
    private NumericExprEval(@Nullable Number value)
    {
      super(value);
    }

    @Override
    public final int asInt()
    {
      if (value == null) {
        return 0;
      }
      return value.intValue();
    }

    @Override
    public final long asLong()
    {
      if (value == null) {
        return 0L;
      }
      return value.longValue();
    }

    @Override
    public final double asDouble()
    {
      if (value == null) {
        return 0.0;
      }
      return value.doubleValue();
    }

    @Override
    public boolean isNumericNull()
    {
      return NullHandling.sqlCompatible() && value == null;
    }
  }

  private static class DoubleExprEval extends NumericExprEval
  {
    private static final DoubleExprEval OF_NULL = new DoubleExprEval(null);

    private DoubleExprEval(@Nullable Number value)
    {
      super(value == null ? null : value.doubleValue());
    }

    @Override
    public final ExpressionType type()
    {
      return ExpressionType.DOUBLE;
    }

    @Override
    public Number valueOrDefault()
    {
      if (value == null) {
        return NullHandling.defaultDoubleValue();
      }
      return value.doubleValue();
    }

    @Override
    public final boolean asBoolean()
    {
      return Evals.asBoolean(asDouble());
    }

    @Nullable
    @Override
    public Object[] asArray()
    {
      return value == null ? null : new Object[]{valueOrDefault().doubleValue()};
    }

    @Override
    public final ExprEval castTo(ExpressionType castTo)
    {
      switch (castTo.getType()) {
        case DOUBLE:
          return this;
        case LONG:
          if (value == null) {
            return ExprEval.ofLong(null);
          } else {
            return ExprEval.of(asLong());
          }
        case STRING:
          return ExprEval.of(asString());
        case ARRAY:
          switch (castTo.getElementType().getType()) {
            case DOUBLE:
              return ExprEval.ofDoubleArray(asArray());
            case LONG:
              return ExprEval.ofLongArray(value == null ? null : new Object[]{value.longValue()});
            case STRING:
              return ExprEval.ofStringArray(value == null ? null : new Object[]{value.toString()});
            default:
              ExpressionType elementType = (ExpressionType) castTo.getElementType();
              return new ArrayExprEval(castTo, new Object[]{castTo(elementType).value()});
          }
        case COMPLEX:
          if (ExpressionType.NESTED_DATA.equals(castTo)) {
            return new NestedDataExprEval(value);
          }
      }
      throw invalidCast(type(), castTo);
    }

    @Override
    public Expr toExpr()
    {
      if (value == null) {
        return new NullDoubleExpr();
      }
      return new DoubleExpr(value.doubleValue());
    }
  }

  private static class LongExprEval extends NumericExprEval
  {
    private static final LongExprEval TRUE = new LongExprEval(Evals.asLong(true));
    private static final LongExprEval FALSE = new LongExprEval(Evals.asLong(false));
    private static final LongExprEval OF_NULL = new LongExprEval(null);

    private LongExprEval(@Nullable Number value)
    {
      super(value == null ? null : value.longValue());
    }

    @Override
    public final ExpressionType type()
    {
      return ExpressionType.LONG;
    }

    @Override
    public Number valueOrDefault()
    {
      if (value == null) {
        return NullHandling.defaultLongValue();
      }
      return value.longValue();
    }

    @Override
    public final boolean asBoolean()
    {
      return Evals.asBoolean(asLong());
    }

    @Nullable
    @Override
    public Object[] asArray()
    {
      return value == null ? null : new Object[]{valueOrDefault().longValue()};
    }

    @Override
    public final ExprEval castTo(ExpressionType castTo)
    {
      switch (castTo.getType()) {
        case DOUBLE:
          if (value == null) {
            return ExprEval.ofDouble(null);
          } else {
            return ExprEval.of(asDouble());
          }
        case LONG:
          return this;
        case STRING:
          return ExprEval.of(asString());
        case ARRAY:
          if (value == null) {
            return new ArrayExprEval(castTo, null);
          }
          switch (castTo.getElementType().getType()) {
            case DOUBLE:
              return ExprEval.ofDoubleArray(new Object[]{value.doubleValue()});
            case LONG:
              return ExprEval.ofLongArray(asArray());
            case STRING:
              return ExprEval.ofStringArray(new Object[]{value.toString()});
            default:
              ExpressionType elementType = (ExpressionType) castTo.getElementType();
              return new ArrayExprEval(castTo, new Object[]{castTo(elementType).value()});
          }
        case COMPLEX:
          if (ExpressionType.NESTED_DATA.equals(castTo)) {
            return new NestedDataExprEval(value);
          }
      }
      throw invalidCast(type(), castTo);
    }

    @Override
    public Expr toExpr()
    {
      if (value == null) {
        return new NullLongExpr();
      }
      return new LongExpr(value.longValue());
    }
  }

  private static class StringExprEval extends ExprEval<String>
  {
    private static final StringExprEval OF_NULL = new StringExprEval(null);

    // Cached primitive values.
    private boolean intValueValid = false;
    private boolean longValueValid = false;
    private boolean doubleValueValid = false;
    private boolean booleanValueValid = false;
    private int intValue;
    private long longValue;
    private double doubleValue;
    private boolean booleanValue;

    @Nullable
    private Number numericVal;

    private StringExprEval(@Nullable String value)
    {
      super(NullHandling.emptyToNullIfNeeded(value));
    }

    @Override
    public final ExpressionType type()
    {
      return ExpressionType.STRING;
    }

    @Override
    public int asInt()
    {
      if (!intValueValid) {
        intValue = computeInt();
        intValueValid = true;
      }

      return intValue;
    }

    @Override
    public long asLong()
    {
      if (!longValueValid) {
        longValue = computeLong();
        longValueValid = true;
      }

      return longValue;
    }

    @Override
    public double asDouble()
    {
      if (!doubleValueValid) {
        doubleValue = computeDouble();
        doubleValueValid = true;
      }

      return doubleValue;
    }

    @Nullable
    @Override
    public String asString()
    {
      return value;
    }

    @Nullable
    @Override
    public Object[] asArray()
    {
      return value == null ? null : new Object[]{value};
    }

    private int computeInt()
    {
      Number number = computeNumber();
      if (number == null) {
        return 0;
      }
      return number.intValue();
    }

    private long computeLong()
    {
      Number number = computeNumber();
      if (number == null) {
        return 0L;
      }
      return number.longValue();
    }

    private double computeDouble()
    {
      Number number = computeNumber();
      if (number == null) {
        return 0.0d;
      }
      return number.doubleValue();
    }

    @Nullable
    Number computeNumber()
    {
      if (value == null) {
        return null;
      }
      if (numericVal != null) {
        // Optimization for non-null case.
        return numericVal;
      }
      numericVal = computeNumber(value);
      return numericVal;
    }

    @Override
    public boolean isNumericNull()
    {
      return computeNumber() == null;
    }

    @Override
    public final boolean asBoolean()
    {
      if (!booleanValueValid) {
        booleanValue = Evals.asBoolean(value);
        booleanValueValid = true;
      }

      return booleanValue;
    }

    @Override
    public final ExprEval castTo(ExpressionType castTo)
    {
      switch (castTo.getType()) {
        case DOUBLE:
          return ExprEval.ofDouble(computeNumber());
        case LONG:
          return ExprEval.ofLong(computeNumber());
        case STRING:
          return this;
        case ARRAY:
          if (value == null) {
            return new ArrayExprEval(castTo, null);
          }
          final Number number = computeNumber();
          switch (castTo.getElementType().getType()) {
            case DOUBLE:
              return ExprEval.ofDoubleArray(
                  new Object[]{number == null ? null : number.doubleValue()}
              );
            case LONG:
              return ExprEval.ofLongArray(
                  new Object[]{number == null ? null : number.longValue()}
              );
            case STRING:
              return ExprEval.ofStringArray(new Object[]{value});
            default:
              ExpressionType elementType = (ExpressionType) castTo.getElementType();
              return new ArrayExprEval(castTo, new Object[]{castTo(elementType).value()});
          }
        case COMPLEX:
          if (ExpressionType.NESTED_DATA.equals(castTo)) {
            return new NestedDataExprEval(value);
          }
      }
      throw invalidCast(type(), castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new StringExpr(value);
    }
  }

  static class ArrayExprEval extends ExprEval<Object[]>
  {
    public static final ExprEval OF_NULL_LONG = new ArrayExprEval(ExpressionType.LONG_ARRAY, null);
    public static final ExprEval OF_NULL_DOUBLE = new ArrayExprEval(ExpressionType.DOUBLE_ARRAY, null);
    public static final ExprEval OF_NULL_STRING = new ArrayExprEval(ExpressionType.STRING_ARRAY, null);

    private final ExpressionType arrayType;

    private ArrayExprEval(ExpressionType arrayType, @Nullable Object[] value)
    {
      super(value);
      this.arrayType = arrayType;
      Preconditions.checkArgument(arrayType.isArray(), "Output type %s is not an array", arrayType);
    }

    @Override
    public ExpressionType type()
    {
      return arrayType;
    }

    @Override
    @Nullable
    public String asString()
    {
      if (!isStringValueCached()) {
        if (value == null) {
          cacheStringValue(null);
        } else if (value.length == 1) {
          cacheStringValue(Evals.asString(value[0]));
        } else {
          cacheStringValue(Arrays.deepToString(value));
        }
      }

      return getCachedStringValue();
    }

    @Override
    public boolean isNumericNull()
    {
      if (isScalar()) {
        if (arrayType.getElementType().is(ExprType.STRING)) {
          Number n = computeNumber((String) getScalarValue());
          return n == null;
        }
        return getScalarValue() == null;
      }

      return true;
    }

    @Override
    public boolean isArray()
    {
      return true;
    }

    @Override
    public int asInt()
    {
      if (isScalar()) {
        Number scalar = null;
        if (arrayType.getElementType().isNumeric()) {
          scalar = (Number) getScalarValue();
        } else if (arrayType.getElementType().is(ExprType.STRING)) {
          scalar = computeNumber((String) getScalarValue());
        }
        if (scalar == null) {
          return 0;
        }
        return scalar.intValue();
      }
      return 0;
    }

    @Override
    public long asLong()
    {
      if (isScalar()) {
        Number scalar = null;
        if (arrayType.getElementType().isNumeric()) {
          scalar = (Number) getScalarValue();
        } else if (arrayType.getElementType().is(ExprType.STRING)) {
          scalar = computeNumber((String) getScalarValue());
        }
        if (scalar == null) {
          return 0;
        }
        return scalar.longValue();
      }
      return 0L;
    }

    @Override
    public double asDouble()
    {
      if (isScalar()) {
        Number scalar = null;
        if (arrayType.getElementType().isNumeric()) {
          scalar = (Number) getScalarValue();
        } else if (arrayType.getElementType().is(ExprType.STRING)) {
          scalar = computeNumber((String) getScalarValue());
        }
        if (scalar == null) {
          return 0.0;
        }
        return scalar.doubleValue();
      }
      return 0.0;
    }

    @Override
    public boolean asBoolean()
    {
      if (isScalar()) {
        if (arrayType.getElementType().isNumeric()) {
          Number scalarValue = (Number) getScalarValue();
          if (scalarValue == null) {
            return false;
          }
          return Evals.asBoolean(scalarValue.longValue());
        }
        if (arrayType.getElementType().is(ExprType.STRING)) {
          return Evals.asBoolean((String) getScalarValue());
        }
      }
      return false;
    }

    @Nullable
    @Override
    public Object[] asArray()
    {
      return value;
    }

    @Override
    public ExprEval castTo(ExpressionType castTo)
    {
      if (value == null) {
        if (castTo.isArray()) {
          return new ArrayExprEval(castTo, null);
        }
        return ExprEval.ofType(castTo, null);
      }
      if (type().equals(castTo)) {
        return this;
      }
      switch (castTo.getType()) {
        case STRING:
          if (value.length == 1) {
            return ExprEval.of(asString());
          }
          break;
        case LONG:
          if (value.length == 1) {
            return isNumericNull() ? ExprEval.ofLong(null) : ExprEval.ofLong(asLong());
          }
          break;
        case DOUBLE:
          if (value.length == 1) {
            return isNumericNull() ? ExprEval.ofDouble(null) : ExprEval.ofDouble(asDouble());
          }
          break;
        case ARRAY:
          ExpressionType elementType = (ExpressionType) castTo.getElementType();
          Object[] cast = new Object[value.length];
          for (int i = 0; i < value.length; i++) {
            cast[i] = ExprEval.ofType(elementType(), value[i]).castTo(elementType).value();
          }
          return ExprEval.ofArray(castTo, cast);
        case COMPLEX:
          if (ExpressionType.NESTED_DATA.equals(castTo)) {
            return new NestedDataExprEval(value);
          }
      }

      throw invalidCast(type(), castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new ArrayExpr(arrayType, value);
    }

    protected boolean isScalar()
    {
      return value != null && value.length == 1;
    }

    @Nullable
    protected Object getScalarValue()
    {
      return value[0];
    }
  }

  private static class ComplexExprEval extends ExprEval<Object>
  {
    private final ExpressionType expressionType;

    private ComplexExprEval(ExpressionType expressionType, @Nullable Object value)
    {
      super(value);
      this.expressionType = expressionType;
    }

    @Override
    public ExpressionType type()
    {
      return expressionType;
    }

    @Override
    public boolean isNumericNull()
    {
      return true;
    }

    @Override
    public int asInt()
    {
      return 0;
    }

    @Override
    public long asLong()
    {
      return 0;
    }

    @Override
    public double asDouble()
    {
      return 0;
    }

    @Override
    public boolean asBoolean()
    {
      return false;
    }
    @Nullable
    @Override
    public Object[] asArray()
    {
      return null;
    }

    @Override
    public ExprEval castTo(ExpressionType castTo)
    {
      if (expressionType.equals(castTo)) {
        return this;
      }
      // allow cast of unknown complex to some other complex type
      if (expressionType.getComplexTypeName() == null && castTo.getType().equals(ExprType.COMPLEX)) {
        return ofComplex(castTo, value);
      }
      throw invalidCast(expressionType, castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new ComplexExpr(expressionType, value);
    }
  }

  private static class NestedDataExprEval extends ExprEval<Object>
  {
    @Nullable
    private Number number;
    private boolean computedNumber = false;

    private NestedDataExprEval(@Nullable Object value)
    {
      super(value);
    }

    @Override
    public ExpressionType type()
    {
      return ExpressionType.NESTED_DATA;
    }

    @Override
    public boolean isNumericNull()
    {
      computeNumber();
      return number == null;
    }

    @Override
    public int asInt()
    {
      computeNumber();
      if (number != null) {
        return number.intValue();
      }
      return 0;
    }

    @Override
    public long asLong()
    {
      computeNumber();
      if (number != null) {
        return number.longValue();
      }
      return 0L;
    }

    @Override
    public double asDouble()
    {
      computeNumber();
      if (number != null) {
        return number.doubleValue();
      }
      return 0.0;
    }

    @Override
    public boolean asBoolean()
    {
      Object val = StructuredData.unwrap(value);
      if (val != null) {
        return Evals.objectAsBoolean(val);
      }
      return false;
    }

    private void computeNumber()
    {
      if (!computedNumber && value != null) {
        computedNumber = true;
        Object val = StructuredData.unwrap(value);
        if (val instanceof Number) {
          number = (Number) val;
        } else if (val instanceof Boolean) {
          number = Evals.asLong((Boolean) val);
        } else if (val instanceof String) {
          number = ExprEval.computeNumber((String) val);
        }
      }
    }

    @Nullable
    @Override
    public Object[] asArray()
    {
      Object val = StructuredData.unwrap(value);
      ExprEval maybeArray = ExprEval.bestEffortOf(val);
      if (maybeArray.type().isPrimitive() || maybeArray.isArray()) {
        return maybeArray.asArray();
      }
      return null;
    }

    @Override
    public ExprEval castTo(ExpressionType castTo)
    {
      if (ExpressionType.NESTED_DATA.equals(castTo)) {
        return this;
      }

      Object val = StructuredData.unwrap(value);
      ExprEval bestEffortOf = ExprEval.bestEffortOf(val);

      if (bestEffortOf.type().isPrimitive() || bestEffortOf.type().isArray()) {
        return bestEffortOf.castTo(castTo);
      }
      throw invalidCast(ExpressionType.NESTED_DATA, castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new ComplexExpr(ExpressionType.NESTED_DATA, value);
    }
  }

  public static Types.InvalidCastException invalidCast(ExpressionType fromType, ExpressionType toType)
  {
    return new Types.InvalidCastException(fromType, toType);
  }
}
