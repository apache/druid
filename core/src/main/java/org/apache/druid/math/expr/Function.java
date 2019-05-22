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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * Do NOT remove "unused" members in this class. They are used by generated Antlr
 */
@SuppressWarnings("unused")
interface Function
{
  String name();

  ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings);

  abstract class SingleParam implements Function
  {
    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }
      Expr expr = args.get(0);
      return eval(expr.eval(bindings));
    }

    protected abstract ExprEval eval(ExprEval param);
  }

  abstract class DoubleParam implements Function
  {
    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] needs 2 arguments", name());
      }
      Expr expr1 = args.get(0);
      Expr expr2 = args.get(1);
      return eval(expr1.eval(bindings), expr2.eval(bindings));
    }

    protected abstract ExprEval eval(ExprEval x, ExprEval y);
  }

  abstract class SingleParamMath extends SingleParam
  {
    @Override
    protected final ExprEval eval(ExprEval param)
    {
      if (NullHandling.sqlCompatible() && param.isNumericNull()) {
        return ExprEval.of(null);
      }
      if (param.type() == ExprType.LONG) {
        return eval(param.asLong());
      } else if (param.type() == ExprType.DOUBLE) {
        return eval(param.asDouble());
      }
      return ExprEval.of(null);
    }

    protected ExprEval eval(long param)
    {
      return eval((double) param);
    }

    protected ExprEval eval(double param)
    {
      return eval((long) param);
    }
  }

  abstract class DoubleParamMath extends DoubleParam
  {
    @Override
    protected final ExprEval eval(ExprEval x, ExprEval y)
    {
      if (x.type() == ExprType.STRING || y.type() == ExprType.STRING) {
        return ExprEval.of(null);
      }
      if (x.type() == ExprType.LONG && y.type() == ExprType.LONG) {
        return eval(x.asLong(), y.asLong());
      } else {
        return eval(x.asDouble(), y.asDouble());
      }
    }

    protected ExprEval eval(long x, long y)
    {
      return eval((double) x, (double) y);
    }

    protected ExprEval eval(double x, double y)
    {
      return eval((long) x, (long) y);
    }
  }

  abstract class DoubleParamString extends DoubleParam
  {
    @Override
    protected final ExprEval eval(ExprEval x, ExprEval y)
    {
      if (x.type() != ExprType.STRING || y.type() != ExprType.LONG) {
        throw new IAE(
            "Function[%s] needs a string as first argument and an integer as second argument",
            name()
        );
      }
      return eval(x.asString(), y.asInt());
    }

    protected abstract ExprEval eval(String x, int y);
  }

  class ParseLong implements Function
  {
    @Override
    public String name()
    {
      return "parse_long";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      final int radix;
      if (args.size() == 1) {
        radix = 10;
      } else if (args.size() == 2) {
        radix = args.get(1).eval(bindings).asInt();
      } else {
        throw new IAE("Function[%s] needs 1 or 2 arguments", name());
      }

      final String input = NullHandling.nullToEmptyIfNeeded(args.get(0).eval(bindings).asString());
      if (input == null) {
        return ExprEval.ofLong(null);
      }

      final long retVal;
      try {
        if (radix == 16 && (input.startsWith("0x") || input.startsWith("0X"))) {
          // Strip leading 0x from hex strings.
          retVal = Long.parseLong(input.substring(2), radix);
        } else {
          retVal = Long.parseLong(input, radix);
        }
      }
      catch (NumberFormatException e) {
        return ExprEval.ofLong(null);
      }

      return ExprEval.of(retVal);
    }
  }

  class Pi implements Function
  {
    private static final double PI = Math.PI;

    @Override
    public String name()
    {
      return "pi";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() >= 1) {
        throw new IAE("Function[%s] needs 0 argument", name());
      }

      return ExprEval.of(PI);
    }
  }

  class Abs extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "abs";
    }

    @Override
    protected ExprEval eval(long param)
    {
      return ExprEval.of(Math.abs(param));
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.abs(param));
    }
  }

  class Acos extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "acos";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.acos(param));
    }
  }

  class Asin extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "asin";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.asin(param));
    }
  }

  class Atan extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "atan";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.atan(param));
    }
  }

  class Cbrt extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cbrt";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cbrt(param));
    }
  }

  class Ceil extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "ceil";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ceil(param));
    }
  }

  class Cos extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cos";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cos(param));
    }
  }

  class Cosh extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cosh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cosh(param));
    }
  }

  class Cot extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "cot";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.cos(param) / Math.sin(param));
    }
  }

  class Div extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "div";
    }

    @Override
    protected ExprEval eval(final long x, final long y)
    {
      return ExprEval.of(x / y);
    }

    @Override
    protected ExprEval eval(final double x, final double y)
    {
      return ExprEval.of((long) (x / y));
    }
  }

  class Exp extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "exp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.exp(param));
    }
  }

  class Expm1 extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "expm1";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.expm1(param));
    }
  }

  class Floor extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "floor";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.floor(param));
    }
  }

  class GetExponent extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "getExponent";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.getExponent(param));
    }
  }

  class Log extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "log";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log(param));
    }
  }

  class Log10 extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "log10";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log10(param));
    }
  }

  class Log1p extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "log1p";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.log1p(param));
    }
  }

  class NextUp extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "nextUp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.nextUp(param));
    }
  }

  class Rint extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "rint";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.rint(param));
    }
  }

  class Round implements Function
  {
    @Override
    public String name()
    {
      return "round";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("Function[%s] needs 1 or 2 arguments", name());
      }

      ExprEval value1 = args.get(0).eval(bindings);
      if (value1.type() != ExprType.LONG && value1.type() != ExprType.DOUBLE) {
        throw new IAE("The first argument to the function[%s] should be integer or double type but get the %s type", name(), value1.type());
      }

      if (args.size() == 1) {
        return eval(value1);
      } else {
        ExprEval value2 = args.get(1).eval(bindings);
        if (value2.type() != ExprType.LONG) {
          throw new IAE("The second argument to the function[%s] should be integer type but get the %s type", name(), value2.type());
        }
        return eval(value1, value2.asInt());
      }
    }

    private ExprEval eval(ExprEval param)
    {
      return eval(param, 0);
    }

    private ExprEval eval(ExprEval param, int scale)
    {
      if (param.type() == ExprType.LONG) {
        return ExprEval.of(BigDecimal.valueOf(param.asLong()).setScale(scale, RoundingMode.HALF_UP).longValue());
      } else if (param.type() == ExprType.DOUBLE) {
        return ExprEval.of(BigDecimal.valueOf(param.asDouble()).setScale(scale, RoundingMode.HALF_UP).doubleValue());
      } else {
        return ExprEval.of(null);
      }
    }
  }

  class Signum extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "signum";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.signum(param));
    }
  }

  class Sin extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "sin";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sin(param));
    }
  }

  class Sinh extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "sinh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sinh(param));
    }
  }

  class Sqrt extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "sqrt";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.sqrt(param));
    }
  }

  class Tan extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "tan";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tan(param));
    }
  }

  class Tanh extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "tanh";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.tanh(param));
    }
  }

  class ToDegrees extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "toDegrees";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toDegrees(param));
    }
  }

  class ToRadians extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "toRadians";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.toRadians(param));
    }
  }

  class Ulp extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "ulp";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.ulp(param));
    }
  }

  class Atan2 extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "atan2";
    }

    @Override
    protected ExprEval eval(double y, double x)
    {
      return ExprEval.of(Math.atan2(y, x));
    }
  }

  class CopySign extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "copySign";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.copySign(x, y));
    }
  }

  class Hypot extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "hypot";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.hypot(x, y));
    }
  }

  class Remainder extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "remainder";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.IEEEremainder(x, y));
    }
  }

  class Max extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "max";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(Math.max(x, y));
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.max(x, y));
    }
  }

  class Min extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "min";
    }

    @Override
    protected ExprEval eval(long x, long y)
    {
      return ExprEval.of(Math.min(x, y));
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.min(x, y));
    }
  }

  class NextAfter extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "nextAfter";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.nextAfter(x, y));
    }
  }

  class Pow extends DoubleParamMath
  {
    @Override
    public String name()
    {
      return "pow";
    }

    @Override
    protected ExprEval eval(double x, double y)
    {
      return ExprEval.of(Math.pow(x, y));
    }
  }

  class Scalb extends DoubleParam
  {
    @Override
    public String name()
    {
      return "scalb";
    }

    @Override
    protected ExprEval eval(ExprEval x, ExprEval y)
    {
      return ExprEval.of(Math.scalb(x.asDouble(), y.asInt()));
    }
  }

  class ConditionFunc implements Function
  {
    @Override
    public String name()
    {
      return "if";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }

      ExprEval x = args.get(0).eval(bindings);
      return x.asBoolean() ? args.get(1).eval(bindings) : args.get(2).eval(bindings);
    }
  }

  /**
   * "Searched CASE" function, similar to {@code CASE WHEN boolean_expr THEN result [ELSE else_result] END} in SQL.
   */
  class CaseSearchedFunc implements Function
  {
    @Override
    public String name()
    {
      return "case_searched";
    }

    @Override
    public ExprEval apply(final List<Expr> args, final Expr.ObjectBinding bindings)
    {
      if (args.size() < 2) {
        throw new IAE("Function[%s] must have at least 2 arguments", name());
      }

      for (int i = 0; i < args.size(); i += 2) {
        if (i == args.size() - 1) {
          // ELSE else_result.
          return args.get(i).eval(bindings);
        } else if (args.get(i).eval(bindings).asBoolean()) {
          // Matching WHEN boolean_expr THEN result
          return args.get(i + 1).eval(bindings);
        }
      }

      return ExprEval.of(null);
    }
  }

  /**
   * "Simple CASE" function, similar to {@code CASE expr WHEN value THEN result [ELSE else_result] END} in SQL.
   */
  class CaseSimpleFunc implements Function
  {
    @Override
    public String name()
    {
      return "case_simple";
    }

    @Override
    public ExprEval apply(final List<Expr> args, final Expr.ObjectBinding bindings)
    {
      if (args.size() < 3) {
        throw new IAE("Function[%s] must have at least 3 arguments", name());
      }

      for (int i = 1; i < args.size(); i += 2) {
        if (i == args.size() - 1) {
          // ELSE else_result.
          return args.get(i).eval(bindings);
        } else if (new BinEqExpr("==", args.get(0), args.get(i)).eval(bindings).asBoolean()) {
          // Matching WHEN value THEN result
          return args.get(i + 1).eval(bindings);
        }
      }

      return ExprEval.of(null);
    }
  }

  class CastFunc extends DoubleParam
  {
    @Override
    public String name()
    {
      return "cast";
    }

    @Override
    protected ExprEval eval(ExprEval x, ExprEval y)
    {
      if (NullHandling.sqlCompatible() && x.value() == null) {
        return ExprEval.of(null);
      }
      ExprType castTo;
      try {
        castTo = ExprType.valueOf(StringUtils.toUpperCase(y.asString()));
      }
      catch (IllegalArgumentException e) {
        throw new IAE("invalid type '%s'", y.asString());
      }
      return x.castTo(castTo);
    }
  }

  class TimestampFromEpochFunc implements Function
  {
    @Override
    public String name()
    {
      return "timestamp";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1 && args.size() != 2) {
        throw new IAE("Function[%s] needs 1 or 2 arguments", name());
      }
      ExprEval value = args.get(0).eval(bindings);
      if (value.type() != ExprType.STRING) {
        throw new IAE("first argument should be string type but got %s type", value.type());
      }

      DateTimes.UtcFormatter formatter = DateTimes.ISO_DATE_OPTIONAL_TIME;
      if (args.size() > 1) {
        ExprEval format = args.get(1).eval(bindings);
        if (format.type() != ExprType.STRING) {
          throw new IAE("second argument should be string type but got %s type", format.type());
        }
        formatter = DateTimes.wrapFormatter(DateTimeFormat.forPattern(format.asString()));
      }
      DateTime date;
      try {
        date = formatter.parse(value.asString());
      }
      catch (IllegalArgumentException e) {
        throw new IAE(e, "invalid value %s", value.asString());
      }
      return toValue(date);
    }

    protected ExprEval toValue(DateTime date)
    {
      return ExprEval.of(date.getMillis());
    }
  }

  class UnixTimestampFunc extends TimestampFromEpochFunc
  {
    @Override
    public String name()
    {
      return "unix_timestamp";
    }

    @Override
    protected final ExprEval toValue(DateTime date)
    {
      return ExprEval.of(date.getMillis() / 1000);
    }
  }

  class NvlFunc implements Function
  {
    @Override
    public String name()
    {
      return "nvl";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 2) {
        throw new IAE("Function[%s] needs 2 arguments", name());
      }
      final ExprEval eval = args.get(0).eval(bindings);
      return eval.value() == null ? args.get(1).eval(bindings) : eval;
    }
  }

  class ConcatFunc implements Function
  {
    @Override
    public String name()
    {
      return "concat";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() == 0) {
        return ExprEval.of(null);
      } else {
        // Pass first argument in to the constructor to provide StringBuilder a little extra sizing hint.
        String first = NullHandling.nullToEmptyIfNeeded(args.get(0).eval(bindings).asString());
        if (first == null) {
          // Result of concatenation is null if any of the Values is null.
          // e.g. 'select CONCAT(null, "abc") as c;' will return null as per Standard SQL spec.
          return ExprEval.of(null);
        }
        final StringBuilder builder = new StringBuilder(first);
        for (int i = 1; i < args.size(); i++) {
          final String s = NullHandling.nullToEmptyIfNeeded(args.get(i).eval(bindings).asString());
          if (s == null) {
            // Result of concatenation is null if any of the Values is null.
            // e.g. 'select CONCAT(null, "abc") as c;' will return null as per Standard SQL spec.
            return ExprEval.of(null);
          } else {
            builder.append(s);
          }
        }
        return ExprEval.of(builder.toString());
      }
    }
  }

  class StrlenFunc implements Function
  {
    @Override
    public String name()
    {
      return "strlen";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }

      final String arg = args.get(0).eval(bindings).asString();
      return arg == null ? ExprEval.ofLong(NullHandling.defaultLongValue()) : ExprEval.of(arg.length());
    }
  }

  class StringFormatFunc implements Function
  {
    @Override
    public String name()
    {
      return "format";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() < 1) {
        throw new IAE("Function[%s] needs 1 or more arguments", name());
      }

      final String formatString = NullHandling.nullToEmptyIfNeeded(args.get(0).eval(bindings).asString());

      if (formatString == null) {
        return ExprEval.of(null);
      }

      final Object[] formatArgs = new Object[args.size() - 1];
      for (int i = 1; i < args.size(); i++) {
        formatArgs[i - 1] = args.get(i).eval(bindings).value();
      }

      return ExprEval.of(StringUtils.nonStrictFormat(formatString, formatArgs));
    }
  }

  class StrposFunc implements Function
  {
    @Override
    public String name()
    {
      return "strpos";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() < 2 || args.size() > 3) {
        throw new IAE("Function[%s] needs 2 or 3 arguments", name());
      }

      final String haystack = NullHandling.nullToEmptyIfNeeded(args.get(0).eval(bindings).asString());
      final String needle = NullHandling.nullToEmptyIfNeeded(args.get(1).eval(bindings).asString());

      if (haystack == null || needle == null) {
        return ExprEval.of(null);
      }

      final int fromIndex;

      if (args.size() >= 3) {
        fromIndex = args.get(2).eval(bindings).asInt();
      } else {
        fromIndex = 0;
      }

      return ExprEval.of(haystack.indexOf(needle, fromIndex));
    }
  }

  class SubstringFunc implements Function
  {
    @Override
    public String name()
    {
      return "substring";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }

      final String arg = args.get(0).eval(bindings).asString();

      if (arg == null) {
        return ExprEval.of(null);
      }

      // Behaves like SubstringDimExtractionFn, not SQL SUBSTRING
      final int index = args.get(1).eval(bindings).asInt();
      final int length = args.get(2).eval(bindings).asInt();

      if (index < arg.length()) {
        if (length >= 0) {
          return ExprEval.of(arg.substring(index, Math.min(index + length, arg.length())));
        } else {
          return ExprEval.of(arg.substring(index));
        }
      } else {
        // If starting index of substring is greater then the length of string, the result will be a zero length string.
        // e.g. 'select substring("abc", 4,5) as c;' will return an empty string
        return ExprEval.of(NullHandling.defaultStringValue());
      }
    }
  }

  class RightFunc extends DoubleParamString
  {
    @Override
    public String name()
    {
      return "right";
    }

    @Override
    protected ExprEval eval(String x, int y)
    {
      if (y < 0) {
        throw new IAE(
            "Function[%s] needs a postive integer as second argument",
            name()
        );
      }
      int len = x.length();
      return ExprEval.of(y < len ? x.substring(len - y) : x);
    }
  }

  class LeftFunc extends DoubleParamString
  {
    @Override
    public String name()
    {
      return "left";
    }

    @Override
    protected ExprEval eval(String x, int y)
    {
      if (y < 0) {
        throw new IAE(
            "Function[%s] needs a postive integer as second argument",
            name()
        );
      }
      return ExprEval.of(y < x.length() ? x.substring(0, y) : x);
    }
  }

  class ReplaceFunc implements Function
  {
    @Override
    public String name()
    {
      return "replace";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }

      final String arg = args.get(0).eval(bindings).asString();
      final String pattern = NullHandling.nullToEmptyIfNeeded(args.get(1).eval(bindings).asString());
      final String replacement = NullHandling.nullToEmptyIfNeeded(args.get(2).eval(bindings).asString());
      if (arg == null) {
        return ExprEval.of(NullHandling.defaultStringValue());
      }
      return ExprEval.of(StringUtils.replace(arg, pattern, replacement));
    }
  }

  class LowerFunc implements Function
  {
    @Override
    public String name()
    {
      return "lower";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }

      final String arg = args.get(0).eval(bindings).asString();
      if (arg == null) {
        return ExprEval.of(NullHandling.defaultStringValue());
      }
      return ExprEval.of(StringUtils.toLowerCase(arg));
    }
  }

  class UpperFunc implements Function
  {
    @Override
    public String name()
    {
      return "upper";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }

      final String arg = args.get(0).eval(bindings).asString();
      if (arg == null) {
        return ExprEval.of(NullHandling.defaultStringValue());
      }
      return ExprEval.of(StringUtils.toUpperCase(arg));
    }
  }

  class ReverseFunc extends SingleParam
  {
    @Override
    public String name()
    {
      return "reverse";
    }

    @Override
    protected ExprEval eval(ExprEval param)
    {
      if (param.type() != ExprType.STRING) {
        throw new IAE(
            "Function[%s] needs a string argument",
            name()
        );
      }
      final String arg = param.asString();
      return ExprEval.of(arg == null ? NullHandling.defaultStringValue() : new StringBuilder(arg).reverse().toString());
    }
  }

  class RepeatFunc extends DoubleParamString
  {
    @Override
    public String name()
    {
      return "repeat";
    }

    @Override
    protected ExprEval eval(String x, int y)
    {
      return ExprEval.of(y < 1 ? NullHandling.defaultStringValue() : StringUtils.repeat(x, y));
    }
  }

  class IsNullFunc implements Function
  {
    @Override
    public String name()
    {
      return "isnull";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }

      final ExprEval expr = args.get(0).eval(bindings);
      return ExprEval.of(expr.value() == null, ExprType.LONG);
    }
  }

  class IsNotNullFunc implements Function
  {
    @Override
    public String name()
    {
      return "notnull";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1) {
        throw new IAE("Function[%s] needs 1 argument", name());
      }

      final ExprEval expr = args.get(0).eval(bindings);
      return ExprEval.of(expr.value() != null, ExprType.LONG);
    }
  }

  class LpadFunc implements Function
  {
    @Override
    public String name()
    {
      return "lpad";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }
      
      String base = args.get(0).eval(bindings).asString();
      int len = args.get(1).eval(bindings).asInt();
      String pad = args.get(2).eval(bindings).asString();

      if (base == null || pad == null) {
        return ExprEval.of(null);
      } else {
        return ExprEval.of(len == 0 ? NullHandling.defaultStringValue() : StringUtils.lpad(base, len, pad));
      }

    }
  }

  class RpadFunc implements Function
  {
    @Override
    public String name()
    {
      return "rpad";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }

      String base = args.get(0).eval(bindings).asString();
      int len = args.get(1).eval(bindings).asInt();
      String pad = args.get(2).eval(bindings).asString();

      if (base == null || pad == null) {
        return ExprEval.of(null);
      } else {
        return ExprEval.of(len == 0 ? NullHandling.defaultStringValue() : StringUtils.rpad(base, len, pad));
      }

    }
  }

  class SubMonthFunc implements Function
  {
    @Override
    public String name()
    {
      return "subtract_months";
    }

    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 3) {
        throw new IAE("Function[%s] needs 3 arguments", name());
      }

      Long left = args.get(0).eval(bindings).asLong();
      Long right = args.get(1).eval(bindings).asLong();
      DateTimeZone timeZone = DateTimes.inferTzFromString(args.get(2).eval(bindings).asString());

      if (left == null || right == null) {
        return ExprEval.of(null);
      } else {
        return ExprEval.of(DateTimes.subMonths(right, left, timeZone));
      }

    }
  }

}
