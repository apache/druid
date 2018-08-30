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

/**
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
    return new StringExprEval(stringValue);
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
    return new StringExprEval(val == null ? null : String.valueOf(val));
  }

  @Nullable
  final T value;

  private ExprEval(T value)
  {
    this.value = value;
  }

  public abstract ExprType type();

  public Object value()
  {
    return value;
  }

  /**
   * returns true if numeric primitive value for this ExprEval is null, otherwise false.
   */
  public abstract boolean isNumericNull();

  public abstract int asInt();

  public abstract long asLong();

  public abstract double asDouble();

  @Nullable
  public String asString()
  {
    return value == null ? null : String.valueOf(value);
  }

  public abstract boolean asBoolean();

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

    @Override
    public final ExprEval castTo(ExprType castTo)
    {
      switch (castTo) {
        case DOUBLE:
          return this;
        case LONG:
          return ExprEval.of(value == null ? null : asLong());
        case STRING:
          return ExprEval.of(asString());
      }
      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
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

    @Override
    public final ExprEval castTo(ExprType castTo)
    {
      switch (castTo) {
        case DOUBLE:
          return ExprEval.of(value == null ? null : asDouble());
        case LONG:
          return this;
        case STRING:
          return ExprEval.of(asString());
      }
      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new LongExpr(value.longValue());
    }
  }

  private static class StringExprEval extends ExprEval<String>
  {
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
    public final int asInt()
    {
      Number number = asNumber();
      if (number == null) {
        assert NullHandling.replaceWithDefault();
        return 0;
      }
      return number.intValue();
    }

    @Override
    public final long asLong()
    {
      Number number = asNumber();
      if (number == null) {
        assert NullHandling.replaceWithDefault();
        return 0L;
      }
      return number.longValue();
    }

    @Override
    public final double asDouble()
    {
      Number number = asNumber();
      if (number == null) {
        assert NullHandling.replaceWithDefault();
        return 0.0d;
      }
      return number.doubleValue();
    }

    @Nullable
    private Number asNumber()
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
      return asNumber() == null;
    }

    @Override
    public final boolean asBoolean()
    {
      return Evals.asBoolean(value);
    }

    @Override
    public final ExprEval castTo(ExprType castTo)
    {
      switch (castTo) {
        case DOUBLE:
          return ExprEval.ofDouble(asNumber());
        case LONG:
          return ExprEval.ofLong(asNumber());
        case STRING:
          return this;
      }
      throw new IAE("invalid type " + castTo);
    }

    @Override
    public Expr toExpr()
    {
      return new StringExpr(value);
    }
  }
}
