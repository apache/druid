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

import com.google.common.primitives.Doubles;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Generic result holder for evaluated {@link Expr} containing the value and {@link ExprType} of the value to allow
 */
public abstract class ExprEval<T>
{
  private static final int NULL_LENGTH = -1;

  /**
   * Deserialize an expression stored in a bytebuffer, e.g. for an agg.
   *
   * This should be refactored to be consolidated with some of the standard type handling of aggregators probably
   */
  public static ExprEval deserialize(ByteBuffer buffer, int position)
  {
    // | expression type (byte) | expression bytes |
    ExprType type = ExprType.fromByte(buffer.get(position));
    int offset = position + 1;
    switch (type) {
      case LONG:
        // | expression type (byte) | is null (byte) | long bytes |
        if (buffer.get(offset++) == NullHandling.IS_NOT_NULL_BYTE) {
          return of(buffer.getLong(offset));
        }
        return ofLong(null);
      case DOUBLE:
        // | expression type (byte) | is null (byte) | double bytes |
        if (buffer.get(offset++) == NullHandling.IS_NOT_NULL_BYTE) {
          return of(buffer.getDouble(offset));
        }
        return ofDouble(null);
      case STRING:
        // | expression type (byte) | string length (int) | string bytes |
        final int length = buffer.getInt(offset);
        if (length < 0) {
          return of(null);
        }
        final byte[] stringBytes = new byte[length];
        final int oldPosition = buffer.position();
        buffer.position(offset + Integer.BYTES);
        buffer.get(stringBytes, 0, length);
        buffer.position(oldPosition);
        return of(StringUtils.fromUtf8(stringBytes));
      case LONG_ARRAY:
        // | expression type (byte) | array length (int) | array bytes |
        final int longArrayLength = buffer.getInt(offset);
        offset += Integer.BYTES;
        if (longArrayLength < 0) {
          return ofLongArray(null);
        }
        final Long[] longs = new Long[longArrayLength];
        for (int i = 0; i < longArrayLength; i++) {
          final byte isNull = buffer.get(offset);
          offset += Byte.BYTES;
          if (isNull == NullHandling.IS_NOT_NULL_BYTE) {
            // | is null (byte) | long bytes |
            longs[i] = buffer.getLong(offset);
            offset += Long.BYTES;
          } else {
            // | is null (byte) |
            longs[i] = null;
          }
        }
        return ofLongArray(longs);
      case DOUBLE_ARRAY:
        // | expression type (byte) | array length (int) | array bytes |
        final int doubleArrayLength = buffer.getInt(offset);
        offset += Integer.BYTES;
        if (doubleArrayLength < 0) {
          return ofDoubleArray(null);
        }
        final Double[] doubles = new Double[doubleArrayLength];
        for (int i = 0; i < doubleArrayLength; i++) {
          final byte isNull = buffer.get(offset);
          offset += Byte.BYTES;
          if (isNull == NullHandling.IS_NOT_NULL_BYTE) {
            // | is null (byte) | double bytes |
            doubles[i] = buffer.getDouble(offset);
            offset += Double.BYTES;
          } else {
            // | is null (byte) |
            doubles[i] = null;
          }
        }
        return ofDoubleArray(doubles);
      case STRING_ARRAY:
        // | expression type (byte) | array length (int) | array bytes |
        final int stringArrayLength = buffer.getInt(offset);
        offset += Integer.BYTES;
        if (stringArrayLength < 0) {
          return ofStringArray(null);
        }
        final String[] stringArray = new String[stringArrayLength];
        for (int i = 0; i < stringArrayLength; i++) {
          final int stringElementLength = buffer.getInt(offset);
          offset += Integer.BYTES;
          if (stringElementLength < 0) {
            // | string length (int) |
            stringArray[i] = null;
          } else {
            // | string length (int) | string bytes |
            final byte[] stringElementBytes = new byte[stringElementLength];
            final int oldPosition2 = buffer.position();
            buffer.position(offset);
            buffer.get(stringElementBytes, 0, stringElementLength);
            buffer.position(oldPosition2);
            stringArray[i] = StringUtils.fromUtf8(stringElementBytes);
            offset += stringElementLength;
          }
        }
        return ofStringArray(stringArray);
      default:
        throw new UOE("how can this be?");
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
    buffer.put(offset++, eval.type().getId());
    switch (eval.type()) {
      case LONG:
        if (eval.isNumericNull()) {
          buffer.put(offset, NullHandling.IS_NULL_BYTE);
        } else {
          buffer.put(offset++, NullHandling.IS_NOT_NULL_BYTE);
          buffer.putLong(offset, eval.asLong());
        }
        break;
      case DOUBLE:
        if (eval.isNumericNull()) {
          buffer.put(offset, NullHandling.IS_NULL_BYTE);
        } else {
          buffer.put(offset++, NullHandling.IS_NOT_NULL_BYTE);
          buffer.putDouble(offset, eval.asDouble());
        }
        break;
      case STRING:
        final byte[] stringBytes = StringUtils.toUtf8Nullable(eval.asString());
        if (stringBytes != null) {
          // | expression type (byte) | string length (int) | string bytes |
          checkMaxBytes(eval.type(), 1 + Integer.BYTES + stringBytes.length, maxSizeBytes);
          buffer.putInt(offset, stringBytes.length);
          offset += Integer.BYTES;
          final int oldPosition = buffer.position();
          buffer.position(offset);
          buffer.put(stringBytes, 0, stringBytes.length);
          buffer.position(oldPosition);
        } else {
          checkMaxBytes(eval.type(), 1 + Integer.BYTES, maxSizeBytes);
          buffer.putInt(offset, NULL_LENGTH);
        }
        break;
      case LONG_ARRAY:
        Long[] longs = eval.asLongArray();
        if (longs == null) {
          // | expression type (byte) | array length (int) |
          checkMaxBytes(eval.type(), 1 + Integer.BYTES, maxSizeBytes);
          buffer.putInt(offset, NULL_LENGTH);
        } else {
          // | expression type (byte) | array length (int) | array bytes |
          final int sizeBytes = 1 + Integer.BYTES + (Long.BYTES * longs.length);
          checkMaxBytes(eval.type(), sizeBytes, maxSizeBytes);
          buffer.putInt(offset, longs.length);
          offset += Integer.BYTES;
          for (Long aLong : longs) {
            if (aLong != null) {
              buffer.put(offset, NullHandling.IS_NOT_NULL_BYTE);
              offset++;
              buffer.putLong(offset, aLong);
              offset += Long.BYTES;
            } else {
              buffer.put(offset++, NullHandling.IS_NULL_BYTE);
            }
          }
        }
        break;
      case DOUBLE_ARRAY:
        Double[] doubles = eval.asDoubleArray();
        if (doubles == null) {
          // | expression type (byte) | array length (int) |
          checkMaxBytes(eval.type(), 1 + Integer.BYTES, maxSizeBytes);
          buffer.putInt(offset, NULL_LENGTH);
        } else {
          // | expression type (byte) | array length (int) | array bytes |
          final int sizeBytes = 1 + Integer.BYTES + (Double.BYTES * doubles.length);
          checkMaxBytes(eval.type(), sizeBytes, maxSizeBytes);
          buffer.putInt(offset, doubles.length);
          offset += Integer.BYTES;

          for (Double aDouble : doubles) {
            if (aDouble != null) {
              buffer.put(offset, NullHandling.IS_NOT_NULL_BYTE);
              offset++;
              buffer.putDouble(offset, aDouble);
              offset += Long.BYTES;
            } else {
              buffer.put(offset++, NullHandling.IS_NULL_BYTE);
            }
          }
        }
        break;
      case STRING_ARRAY:
        String[] strings = eval.asStringArray();
        if (strings == null) {
          // | expression type (byte) | array length (int) |
          checkMaxBytes(eval.type(), 1 + Integer.BYTES, maxSizeBytes);
          buffer.putInt(offset, NULL_LENGTH);
        } else {
          // | expression type (byte) | array length (int) | array bytes |
          buffer.putInt(offset, strings.length);
          offset += Integer.BYTES;
          int sizeBytes = 1 + Integer.BYTES;
          for (String string : strings) {
            if (string == null) {
              // | string length (int) |
              sizeBytes += Integer.BYTES;
              checkMaxBytes(eval.type(), sizeBytes, maxSizeBytes);
              buffer.putInt(offset, NULL_LENGTH);
              offset += Integer.BYTES;
            } else {
              // | string length (int) | string bytes |
              final byte[] stringElementBytes = StringUtils.toUtf8(string);
              sizeBytes += Integer.BYTES + stringElementBytes.length;
              checkMaxBytes(eval.type(), sizeBytes, maxSizeBytes);
              buffer.putInt(offset, stringElementBytes.length);
              offset += Integer.BYTES;
              final int oldPosition = buffer.position();
              buffer.position(offset);
              buffer.put(stringElementBytes, 0, stringElementBytes.length);
              buffer.position(oldPosition);
              offset += stringElementBytes.length;
            }
          }
        }
        break;
      default:
        throw new UOE("how can this be?");
    }
  }

  private static void checkMaxBytes(ExprType type, int sizeBytes, int maxSizeBytes)
  {
    if (sizeBytes > maxSizeBytes) {
      throw new ISE("Unable to serialize [%s], size [%s] is larger than max [%s]", type, sizeBytes, maxSizeBytes);
    }
  }

  /**
   * Converts a List to an appropriate array type, optionally doing some conversion to make multi-valued strings
   * consistent across selector types, which are not consistent in treatment of null, [], and [null].
   *
   * If homogenizeMultiValueStrings is true, null and [] will be converted to [null], otherwise they will retain
   */
  @Nullable
  public static Object coerceListToArray(@Nullable List<?> val, boolean homogenizeMultiValueStrings)
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
        return val.stream().map(x -> x != null ? ((Number) x).longValue() : null).toArray(Long[]::new);
      }
      if (coercedType == Float.class || coercedType == Double.class) {
        return val.stream().map(x -> x != null ? ((Number) x).doubleValue() : null).toArray(Double[]::new);
      }
      // default to string
      return val.stream().map(x -> x != null ? x.toString() : null).toArray(String[]::new);
    }
    if (homogenizeMultiValueStrings) {
      return new String[]{null};
    } else {
      if (val != null) {
        return val.toArray();
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

  public static ExprEval ofLong(@Nullable Number longValue)
  {
    return new LongExprEval(longValue);
  }

  public static ExprEval of(long longValue)
  {
    return new LongExprEval(longValue);
  }

  public static ExprEval ofDouble(@Nullable Number doubleValue)
  {
    return new DoubleExprEval(doubleValue);
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

  public static ExprEval ofLongArray(@Nullable Long[] longValue)
  {
    return new LongArrayExprEval(longValue);
  }

  public static ExprEval ofDoubleArray(@Nullable Double[] doubleValue)
  {
    return new DoubleArrayExprEval(doubleValue);
  }

  public static ExprEval ofStringArray(@Nullable String[] stringValue)
  {
    return new StringArrayExprEval(stringValue);
  }

  public static ExprEval of(boolean value, ExprType type)
  {
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

  public static ExprEval ofLongBoolean(boolean value)
  {
    return ExprEval.of(Evals.asLong(value));
  }

  public static ExprEval bestEffortOf(@Nullable Object val)
  {
    if (val instanceof ExprEval) {
      return (ExprEval) val;
    }
    if (val instanceof Number) {
      if (val instanceof Float || val instanceof Double) {
        return new DoubleExprEval((Number) val);
      }
      return new LongExprEval((Number) val);
    }
    if (val instanceof Long[]) {
      return new LongArrayExprEval((Long[]) val);
    }
    if (val instanceof Double[]) {
      return new DoubleArrayExprEval((Double[]) val);
    }
    if (val instanceof Float[]) {
      return new DoubleArrayExprEval(Arrays.stream((Float[]) val).map(Float::doubleValue).toArray(Double[]::new));
    }
    if (val instanceof String[]) {
      return new StringArrayExprEval((String[]) val);
    }

    if (val instanceof List) {
      // do not convert empty lists to arrays with a single null element here, because that should have been done
      // by the selectors preparing their ObjectBindings if necessary. If we get to this point it was legitimately
      // empty
      return bestEffortOf(coerceListToArray((List<?>) val, false));
    }

    return new StringExprEval(val == null ? null : String.valueOf(val));
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

  public abstract ExprType type();

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
   * returns true if numeric primitive value for this ExprEval is null, otherwise false.
   */
  public abstract boolean isNumericNull();

  public boolean isArray()
  {
    return false;
  }

  public abstract int asInt();

  public abstract long asLong();

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

  public abstract ExprEval castTo(ExprType castTo);

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
    private DoubleExprEval(@Nullable Number value)
    {
      super(value == null ? NullHandling.defaultDoubleValue() : (Double) value.doubleValue());
    }

    @Override
    public final ExprType type()
    {
      return ExprType.DOUBLE;
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
    public final ExprEval castTo(ExprType castTo)
    {
      switch (castTo) {
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
        case DOUBLE_ARRAY:
          return ExprEval.ofDoubleArray(asDoubleArray());
        case LONG_ARRAY:
          return ExprEval.ofLongArray(asLongArray());
        case STRING_ARRAY:
          return ExprEval.ofStringArray(asStringArray());
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
    private LongExprEval(@Nullable Number value)
    {
      super(value == null ? NullHandling.defaultLongValue() : (Long) value.longValue());
    }

    @Override
    public final ExprType type()
    {
      return ExprType.LONG;
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
    public final ExprEval castTo(ExprType castTo)
    {
      switch (castTo) {
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
        case DOUBLE_ARRAY:
          return ExprEval.ofDoubleArray(asDoubleArray());
        case LONG_ARRAY:
          return ExprEval.ofLongArray(asLongArray());
        case STRING_ARRAY:
          return ExprEval.ofStringArray(asStringArray());
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
    // Cached primitive values.
    private boolean intValueValid = false;
    private boolean longValueValid = false;
    private boolean doubleValueValid = false;
    private boolean booleanValueValid = false;
    private int intValue;
    private long longValue;
    private double doubleValue;
    private boolean booleanValue;

    private static final StringExprEval OF_NULL = new StringExprEval(null);

    @Nullable
    private Number numericVal;

    private StringExprEval(@Nullable String value)
    {
      super(NullHandling.emptyToNullIfNeeded(value));
    }

    @Override
    public final ExprType type()
    {
      return ExprType.STRING;
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
    public final ExprEval castTo(ExprType castTo)
    {
      switch (castTo) {
        case DOUBLE:
          return ExprEval.ofDouble(computeNumber());
        case LONG:
          return ExprEval.ofLong(computeNumber());
        case STRING:
          return this;
        case DOUBLE_ARRAY:
          return ExprEval.ofDoubleArray(asDoubleArray());
        case LONG_ARRAY:
          return ExprEval.ofLongArray(asLongArray());
        case STRING_ARRAY:
          return ExprEval.ofStringArray(asStringArray());
      }
      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new StringExpr(value);
    }
  }

  abstract static class ArrayExprEval<T> extends ExprEval<T[]>
  {
    private ArrayExprEval(@Nullable T[] value)
    {
      super(value);
    }

    @Override
    @Nullable
    public String asString()
    {
      if (!isStringValueCached()) {
        if (value == null) {
          cacheStringValue(null);
        } else {
          cacheStringValue(Arrays.toString(value));
        }
      }

      return getCachedStringValue();
    }

    @Override
    public boolean isNumericNull()
    {
      return false;
    }

    @Override
    public boolean isArray()
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
    public T[] asArray()
    {
      return value;
    }

    @Nullable
    public T getIndex(int index)
    {
      return value == null ? null : value[index];
    }
  }

  private static class LongArrayExprEval extends ArrayExprEval<Long>
  {
    private LongArrayExprEval(@Nullable Long[] value)
    {
      super(value);
    }

    @Override
    public ExprType type()
    {
      return ExprType.LONG_ARRAY;
    }

    @Nullable
    @Override
    public String[] asStringArray()
    {
      return value == null ? null : Arrays.stream(value).map(x -> x != null ? x.toString() : null).toArray(String[]::new);
    }

    @Nullable
    @Override
    public Long[] asLongArray()
    {
      return value;
    }

    @Nullable
    @Override
    public Double[] asDoubleArray()
    {
      return value == null ? null : Arrays.stream(value).map(Long::doubleValue).toArray(Double[]::new);
    }

    @Override
    public ExprEval castTo(ExprType castTo)
    {
      if (value == null) {
        return StringExprEval.OF_NULL;
      }
      switch (castTo) {
        case LONG_ARRAY:
          return this;
        case DOUBLE_ARRAY:
          return ExprEval.ofDoubleArray(asDoubleArray());
        case STRING_ARRAY:
          return ExprEval.ofStringArray(asStringArray());
      }

      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new LongArrayExpr(value);
    }
  }

  private static class DoubleArrayExprEval extends ArrayExprEval<Double>
  {
    private DoubleArrayExprEval(@Nullable Double[] value)
    {
      super(value);
    }

    @Override
    public ExprType type()
    {
      return ExprType.DOUBLE_ARRAY;
    }

    @Nullable
    @Override
    public String[] asStringArray()
    {
      return value == null ? null : Arrays.stream(value).map(x -> x != null ? x.toString() : null).toArray(String[]::new);
    }

    @Nullable
    @Override
    public Long[] asLongArray()
    {
      return value == null ? null : Arrays.stream(value).map(Double::longValue).toArray(Long[]::new);
    }

    @Nullable
    @Override
    public Double[] asDoubleArray()
    {
      return value;
    }

    @Override
    public ExprEval castTo(ExprType castTo)
    {
      if (value == null) {
        return StringExprEval.OF_NULL;
      }
      switch (castTo) {
        case LONG_ARRAY:
          return ExprEval.ofLongArray(asLongArray());
        case DOUBLE_ARRAY:
          return this;
        case STRING_ARRAY:
          return ExprEval.ofStringArray(asStringArray());
      }

      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new DoubleArrayExpr(value);
    }
  }

  private static class StringArrayExprEval extends ArrayExprEval<String>
  {
    private boolean longValueValid = false;
    private boolean doubleValueValid = false;
    private Long[] longValues;
    private Double[] doubleValues;

    private StringArrayExprEval(@Nullable String[] value)
    {
      super(value);
    }

    @Override
    public ExprType type()
    {
      return ExprType.STRING_ARRAY;
    }

    @Nullable
    @Override
    public String[] asStringArray()
    {
      return value;
    }

    @Nullable
    @Override
    public Long[] asLongArray()
    {
      if (!longValueValid) {
        longValues = computeLongs();
        longValueValid = true;
      }
      return longValues;
    }

    @Nullable
    @Override
    public Double[] asDoubleArray()
    {
      if (!doubleValueValid) {
        doubleValues = computeDoubles();
        doubleValueValid = true;
      }
      return doubleValues;
    }

    @Override
    public ExprEval castTo(ExprType castTo)
    {
      if (value == null) {
        return StringExprEval.OF_NULL;
      }
      switch (castTo) {
        case STRING_ARRAY:
          return this;
        case LONG_ARRAY:
          return ExprEval.ofLongArray(asLongArray());
        case DOUBLE_ARRAY:
          return ExprEval.ofDoubleArray(asDoubleArray());
      }
      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new StringArrayExpr(value);
    }

    @Nullable
    private Long[] computeLongs()
    {
      if (value == null) {
        return null;
      }
      return Arrays.stream(value).map(value -> {
        Long lv = GuavaUtils.tryParseLong(value);
        if (lv == null) {
          Double d = Doubles.tryParse(value);
          if (d != null) {
            lv = d.longValue();
          }
        }
        return lv;
      }).toArray(Long[]::new);
    }

    @Nullable
    private Double[] computeDoubles()
    {
      if (value == null) {
        return null;
      }
      return Arrays.stream(value).map(Doubles::tryParse).toArray(Double[]::new);
    }
  }
}
