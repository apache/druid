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
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.column.ObjectByteStrategy;
import org.apache.druid.segment.column.Types;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Generic result holder for evaluated {@link Expr} containing the value and {@link ExprType} of the value to allow
 */
public abstract class ExprEval<T>
{
  /**
   * Deserialize an expression stored in a bytebuffer, e.g. for an agg.
   *
   * This should be refactored to be consolidated with some of the standard type handling of aggregators probably
   */
  public static ExprEval deserialize(ByteBuffer buffer, int offset, ExpressionType type)
  {
    switch (type.getType()) {
      case LONG:
        if (Types.isNullableNull(buffer, offset)) {
          return ofLong(null);
        }
        return of(Types.readNullableLong(buffer, offset));
      case DOUBLE:
        if (Types.isNullableNull(buffer, offset)) {
          return ofDouble(null);
        }
        return of(Types.readNullableDouble(buffer, offset));
      case STRING:
        if (Types.isNullableNull(buffer, offset)) {
          return of(null);
        }
        final byte[] stringBytes = Types.readNullableVariableBlob(buffer, offset);
        return of(StringUtils.fromUtf8(stringBytes));
      case ARRAY:
        switch (type.getElementType().getType()) {
          case LONG:
            return ofLongArray(Types.readNullableLongArray(buffer, offset));
          case DOUBLE:
            return ofDoubleArray(Types.readNullableDoubleArray(buffer, offset));
          case STRING:
            return ofStringArray(Types.readNullableStringArray(buffer, offset));
          default:
            throw new UOE("Cannot deserialize expression array of type %s", type);
        }
      case COMPLEX:
        return ofComplex(type, Types.readNullableComplexType(buffer, offset, type));
      default:
        throw new UOE("Cannot deserialize expression type %s", type);
    }
  }

  /**
   * Write an expression result to a bytebuffer, throwing an {@link ISE} if the data exceeds a maximum size. Primitive
   * numeric types are not validated to be lower than max size, so it is expected to be at least 10 bytes. Callers
   * of this method should enforce this themselves (instead of doing it here, which might be done every row)
   *
   * This should be refactored to be consolidated with some of the standard type handling of aggregators probably
   */
  public static void serialize(ByteBuffer buffer, int position, ExprEval<?> eval, int maxSizeBytes)
  {
    int offset = position;
    switch (eval.type().getType()) {
      case LONG:
        if (eval.isNumericNull()) {
          Types.writeNull(buffer, offset);
        } else {
          Types.writeNullableLong(buffer, offset, eval.asLong());
        }
        break;
      case DOUBLE:
        if (eval.isNumericNull()) {
          Types.writeNull(buffer, offset);
        } else {
          Types.writeNullableDouble(buffer, offset, eval.asDouble());
        }
        break;
      case STRING:
        final byte[] stringBytes = StringUtils.toUtf8Nullable(eval.asString());
        if (stringBytes != null) {
          Types.writeNullableVariableBlob(buffer, offset, stringBytes, eval.type(), maxSizeBytes);
        } else {
          Types.writeNull(buffer, offset);
        }
        break;
      case ARRAY:
        switch (eval.type().getElementType().getType()) {
          case LONG:
            Long[] longs = eval.asLongArray();
            Types.writeNullableLongArray(buffer, offset, longs, maxSizeBytes);
            break;
          case DOUBLE:
            Double[] doubles = eval.asDoubleArray();
            Types.writeNullableDoubleArray(buffer, offset, doubles, maxSizeBytes);
            break;
          case STRING:
            String[] strings = eval.asStringArray();
            Types.writeNullableStringArray(buffer, offset, strings, maxSizeBytes);
            break;
          default:
            throw new UOE("Cannot serialize expression array type %s", eval.type());
        }
        break;
      case COMPLEX:
        Types.writeNullableComplexType(buffer, offset, eval.type(), eval.value(), maxSizeBytes);
        break;
      default:
        throw new UOE("Cannot serialize expression type %s", eval.type());
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
        return new NonnullPair<>(
            ExpressionType.LONG_ARRAY,
            val.stream().map(x -> x != null ? ((Number) x).longValue() : null).toArray()
        );
      }
      if (coercedType == Float.class || coercedType == Double.class) {
        return new NonnullPair<>(
            ExpressionType.DOUBLE_ARRAY,
            val.stream().map(x -> x != null ? ((Number) x).doubleValue() : null).toArray()
        );
      }
      // default to string
      return new NonnullPair<>(
          ExpressionType.STRING_ARRAY,
          val.stream().map(x -> x != null ? x.toString() : null).toArray()
      );
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

  @Nullable
  public static ExpressionType findArrayType(@Nullable Object[] val)
  {
    // if value is not null and has at least 1 element, conversion is unambigous regardless of the selector
    if (val != null && val.length > 0) {
      Class<?> coercedType = null;

      for (Object elem : val) {
        if (elem != null) {
          coercedType = convertType(coercedType, elem.getClass());
        }
      }

      if (coercedType == Long.class || coercedType == Integer.class) {
        return ExpressionType.LONG_ARRAY;
      }
      if (coercedType == Float.class || coercedType == Double.class) {
        return ExpressionType.DOUBLE_ARRAY;
      }
      // default to string
      return ExpressionType.STRING_ARRAY;
    }
    return null;
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
    if (Number.class.isAssignableFrom(next) || next == String.class) {
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
    throw new UOE("Invalid array expression type: %s", next);
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

  public static ExprEval ofLongArray(@Nullable Long[] longValue)
  {
    if (longValue == null) {
      return ArrayExprEval.OF_NULL_LONG;
    }
    return new ArrayExprEval(ExpressionType.LONG_ARRAY, longValue);
  }

  public static ExprEval ofDoubleArray(@Nullable Double[] doubleValue)
  {
    if (doubleValue == null) {
      return ArrayExprEval.OF_NULL_DOUBLE;
    }
    return new ArrayExprEval(ExpressionType.DOUBLE_ARRAY, doubleValue);
  }

  public static ExprEval ofStringArray(@Nullable String[] stringValue)
  {
    if (stringValue == null) {
      return ArrayExprEval.OF_NULL_STRING;
    }
    return new ArrayExprEval(ExpressionType.STRING_ARRAY, stringValue);
  }


  public static ExprEval ofArray(ExpressionType outputType, Object[] value)
  {
    Preconditions.checkArgument(outputType.isArray());
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
  public static ExprEval ofBoolean(boolean value, ExprType type)
  {
    assert !ExpressionProcessing.useStrictBooleans();
    switch (type) {
      case DOUBLE:
        return ExprEval.of(Evals.asDouble(value));
      case LONG:
        return ExprEval.of(Evals.asLong(value));
      case STRING:
        return ExprEval.of(String.valueOf(value));
      default:
        throw new IllegalArgumentException("invalid type " + type);
    }
  }

  /**
   * Convert a boolean into a long expression type
   */
  public static ExprEval ofLongBoolean(boolean value)
  {
    return ExprEval.of(Evals.asLong(value));
  }

  public static ExprEval ofComplex(ExpressionType outputType, @Nullable Object value)
  {
    return new ComplexExprEval(outputType, value);
  }

  /**
   * Examine java type to find most appropriate expression type
   */
  public static ExprEval bestEffortOf(@Nullable Object val)
  {
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
    if (val instanceof Long[]) {
      return new ArrayExprEval(ExpressionType.LONG_ARRAY, (Long[]) val);
    }
    if (val instanceof Double[]) {
      return new ArrayExprEval(ExpressionType.DOUBLE_ARRAY, (Double[]) val);
    }
    if (val instanceof Float[]) {
      return new ArrayExprEval(ExpressionType.DOUBLE_ARRAY, Arrays.stream((Float[]) val).map(Float::doubleValue).toArray());
    }
    if (val instanceof String[]) {
      return new ArrayExprEval(ExpressionType.STRING_ARRAY, (String[]) val);
    }
    if (val instanceof Object[]) {
      ExpressionType arrayType = findArrayType((Object[]) val);
      if (arrayType != null) {
        return new ArrayExprEval(arrayType, (Object[]) val);
      }
      // default to string if array is empty
      return new ArrayExprEval(ExpressionType.STRING_ARRAY, (Object[]) val);
    }

    if (val instanceof List) {
      // do not convert empty lists to arrays with a single null element here, because that should have been done
      // by the selectors preparing their ObjectBindings if necessary. If we get to this point it was legitimately
      // empty
      NonnullPair<ExpressionType, Object[]> coerced = coerceListToArray((List<?>) val, false);
      if (coerced == null) {
        return bestEffortOf(null);
      }
      return ofArray(coerced.lhs, coerced.rhs);
    }

    if (val != null) {
      // is this cool?
      return new ComplexExprEval(ExpressionType.UNKNOWN_COMPLEX, val);
    }

    return new StringExprEval(null);
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
          return new ArrayExprEval(ExpressionType.STRING_ARRAY, (String[]) value);
        }
        if (value instanceof Object[]) {
          return new ArrayExprEval(ExpressionType.STRING_ARRAY, (Object[]) value);
        }
        if (value instanceof List) {
          return bestEffortOf(value);
        }
        return of((String) value);
      case LONG:
        return ofLong((Number) value);
      case DOUBLE:
        return ofDouble((Number) value);
      case COMPLEX:
        byte[] bytes = null;
        if (value instanceof String) {
          bytes = StringUtils.decodeBase64String((String) value);
        } else if (value instanceof byte[]) {
          bytes = (byte[]) value;
        }

        if (bytes != null) {
          ObjectByteStrategy<?> strategy = Types.getStrategy(type.getComplexTypeName());
          assert strategy != null;
          ByteBuffer bb = ByteBuffer.wrap(bytes);
          return ofComplex(type, strategy.fromByteBuffer(bb, bytes.length));
        }

        return ofComplex(type, value);
      case ARRAY:
        if (value instanceof Object[]) {
          return ofArray(type, (Object[]) value);
        }
        // in a better world, we might get an object that matches the type signature for arrays and could do a switch
        // statement here, but this is not that world yet, and things that are array typed might also be non-arrays,
        // e.g. we might get a String instead of String[], so just fallback to bestEffortOf
        return bestEffortOf(value);
    }
    throw new IAE("Cannot create type [%s]", type);
  }

  @Nullable
  public static Number computeNumber(@Nullable String value)
  {
    if (value == null) {
      return null;
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

  void cacheStringValue(@Nullable String value)
  {
    stringValue = value;
    stringValueCached = true;
  }

  @Nullable
  String getCachedStringValue()
  {
    assert stringValueCached;
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
      if (value == null) {
        stringValue = null;
      } else {
        stringValue = String.valueOf(value);
      }

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

  @Nullable
  public abstract String[] asStringArray();

  @Nullable
  public abstract Long[] asLongArray();

  @Nullable
  public abstract Double[] asDoubleArray();

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
      return value.intValue();
    }

    @Override
    public final long asLong()
    {
      return value.longValue();
    }

    @Override
    public final double asDouble()
    {
      return value.doubleValue();
    }

    @Nullable
    @Override
    public String[] asStringArray()
    {
      return isNumericNull() ? null : new String[] {value.toString()};
    }

    @Nullable
    @Override
    public Long[] asLongArray()
    {
      return isNumericNull() ? null : new Long[] {value.longValue()};
    }

    @Nullable
    @Override
    public Double[] asDoubleArray()
    {
      return isNumericNull() ? null : new Double[] {value.doubleValue()};
    }

    @Override
    public boolean isNumericNull()
    {
      return value == null;
    }
  }

  private static class DoubleExprEval extends NumericExprEval
  {
    private static final DoubleExprEval OF_NULL = new DoubleExprEval(null);

    private DoubleExprEval(@Nullable Number value)
    {
      super(value == null ? NullHandling.defaultDoubleValue() : (Double) value.doubleValue());
    }

    @Override
    public final ExpressionType type()
    {
      return ExpressionType.DOUBLE;
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
      return asDoubleArray();
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
              return ExprEval.ofDoubleArray(asDoubleArray());
            case LONG:
              return ExprEval.ofLongArray(asLongArray());
            case STRING:
              return ExprEval.ofStringArray(asStringArray());
          }
      }
      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      if (isNumericNull()) {
        return new NullDoubleExpr();
      }
      return new DoubleExpr(value.doubleValue());
    }
  }

  private static class LongExprEval extends NumericExprEval
  {
    private static final LongExprEval OF_NULL = new LongExprEval(null);

    private LongExprEval(@Nullable Number value)
    {
      super(value == null ? NullHandling.defaultLongValue() : (Long) value.longValue());
    }

    @Override
    public final ExpressionType type()
    {
      return ExpressionType.LONG;
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
      return asLongArray();
    }

    @Nullable
    @Override
    public Long[] asLongArray()
    {
      return isNumericNull() ? null : new Long[]{value.longValue()};
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
          switch (castTo.getElementType().getType()) {
            case DOUBLE:
              return ExprEval.ofDoubleArray(asDoubleArray());
            case LONG:
              return ExprEval.ofLongArray(asLongArray());
            case STRING:
              return ExprEval.ofStringArray(asStringArray());
          }
      }
      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      if (isNumericNull()) {
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
      return asStringArray();
    }

    private int computeInt()
    {
      Number number = computeNumber();
      if (number == null) {
        assert NullHandling.replaceWithDefault();
        return 0;
      }
      return number.intValue();
    }

    private long computeLong()
    {
      Number number = computeNumber();
      if (number == null) {
        assert NullHandling.replaceWithDefault();
        return 0L;
      }
      return number.longValue();
    }

    private double computeDouble()
    {
      Number number = computeNumber();
      if (number == null) {
        assert NullHandling.replaceWithDefault();
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

    @Nullable
    @Override
    public String[] asStringArray()
    {
      return value == null ? null : new String[] {value};
    }

    @Nullable
    @Override
    public Long[] asLongArray()
    {
      return value == null ? null : new Long[] {computeLong()};
    }

    @Nullable
    @Override
    public Double[] asDoubleArray()
    {
      return value == null ? null : new Double[] {computeDouble()};
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
          switch (castTo.getElementType().getType()) {
            case DOUBLE:
              return ExprEval.ofDoubleArray(asDoubleArray());
            case LONG:
              return ExprEval.ofLongArray(asLongArray());
            case STRING:
              return ExprEval.ofStringArray(asStringArray());
          }
      }
      throw new IAE("invalid type " + castTo);
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
      Preconditions.checkArgument(arrayType.isArray());
      ExpressionType.checkNestedArrayAllowed(arrayType);
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
          if (value[0] == null) {
            cacheStringValue(null);
          } else {
            cacheStringValue(String.valueOf(value[0]));
          }
        } else {
          cacheStringValue(Arrays.toString(value));
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
          assert NullHandling.replaceWithDefault();
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
          assert NullHandling.replaceWithDefault();
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
          assert NullHandling.replaceWithDefault();
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
            assert NullHandling.replaceWithDefault();
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

    @Nullable
    @Override
    public String[] asStringArray()
    {
      if (value != null) {
        if (arrayType.getElementType().is(ExprType.STRING)) {
          return Arrays.stream(value).map(v -> (String) v).toArray(String[]::new);
        } else if (arrayType.getElementType().isNumeric()) {
          return Arrays.stream(value).map(x -> x != null ? x.toString() : null).toArray(String[]::new);
        }
      }
      return null;
    }

    @Nullable
    @Override
    public Long[] asLongArray()
    {
      if (arrayType.getElementType().is(ExprType.LONG)) {
        return Arrays.stream(value).map(v -> (Long) v).toArray(Long[]::new);
      } else if (arrayType.getElementType().is(ExprType.DOUBLE)) {
        return value == null ? null : Arrays.stream(value).map(v -> ((Double) v).longValue()).toArray(Long[]::new);
      } else if (arrayType.getElementType().is(ExprType.STRING)) {
        return Arrays.stream(value).map(v -> {
          if (v == null) {
            return null;
          }
          Long lv = GuavaUtils.tryParseLong((String) v);
          if (lv == null) {
            Double d = Doubles.tryParse((String) v);
            if (d != null) {
              lv = d.longValue();
            }
          }
          return lv;
        }).toArray(Long[]::new);
      }
      return null;
    }

    @Nullable
    @Override
    public Double[] asDoubleArray()
    {
      if (arrayType.getElementType().is(ExprType.DOUBLE)) {
        return Arrays.stream(value).map(v -> (Double) v).toArray(Double[]::new);
      } else if (arrayType.getElementType().is(ExprType.LONG)) {
        return value == null ? null : Arrays.stream(value).map(v -> ((Long) v).doubleValue()).toArray(Double[]::new);
      } else if (arrayType.getElementType().is(ExprType.STRING)) {
        if (value == null) {
          return null;
        }
        return Arrays.stream(value).map(val -> {
          if (val == null) {
            return null;
          }
          return Doubles.tryParse((String) val);
        }).toArray(Double[]::new);
      }
      return new Double[0];
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
      }

      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new ArrayExpr(arrayType, value);
    }

    @Nullable
    public Object getIndex(int index)
    {
      return value == null ? null : value[index];
    }

    protected boolean isScalar()
    {
      return value != null && value.length == 1;
    }

    @Nullable
    protected Object getScalarValue()
    {
      assert value != null && value.length == 1;
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
      return false;
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
      return new Object[0];
    }

    @Nullable
    @Override
    public String[] asStringArray()
    {
      return new String[0];
    }

    @Nullable
    @Override
    public Long[] asLongArray()
    {
      return new Long[0];
    }

    @Nullable
    @Override
    public Double[] asDoubleArray()
    {
      return new Double[0];
    }

    @Override
    public ExprEval castTo(ExpressionType castTo)
    {
      if (expressionType.equals(castTo)) {
        return this;
      }
      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new ComplexExpr(expressionType, value);
    }
  }
}
