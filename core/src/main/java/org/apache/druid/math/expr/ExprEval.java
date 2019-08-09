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

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * Generic result holder for evaluated {@link Expr} containing the value and {@link ExprType} of the value to allow
 */
public abstract class ExprEval<T>
{
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

    return new StringExprEval(val == null ? null : String.valueOf(val));
  }

  // Cached String values
  private boolean stringValueValid = false;
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

  @Nullable
  public String asString()
  {
    if (!stringValueValid) {
      if (value == null) {
        stringValue = null;
      } else {
        stringValue = String.valueOf(value);
      }

      stringValueValid = true;
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
      super(value == null ? NullHandling.defaultDoubleValue() : value);
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
      super(value == null ? NullHandling.defaultLongValue() : value);
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
    private Number computeNumber()
    {
      if (value == null) {
        return null;
      }
      if (numericVal != null) {
        // Optimization for non-null case.
        return numericVal;
      }
      Number rv;
      Long v = GuavaUtils.tryParseLong(value);
      // Do NOT use ternary operator here, because it makes Java to convert Long to Double
      if (v != null) {
        rv = v;
      } else {
        rv = Doubles.tryParse(value);
      }

      numericVal = rv;
      return rv;
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
