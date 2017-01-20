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

package io.druid.math.expr;

import io.druid.java.util.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import com.google.common.base.Supplier;

import java.util.List;

/**
 */
interface Function
{
  String name();

  ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings);

  // optional interface to be used when function should be created per reference in expression
  interface FunctionFactory extends Supplier<Function>, Function {
  }

  abstract class SingleParam implements Function
  {
    @Override
    public ExprEval apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1) {
        throw new IAE("function '%s' needs 1 argument", name());
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
        throw new IAE("function '%s' needs 2 arguments", name());
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

  class Round extends SingleParamMath
  {
    @Override
    public String name()
    {
      return "round";
    }

    @Override
    protected ExprEval eval(double param)
    {
      return ExprEval.of(Math.round(param));
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
        throw new IAE("function 'if' needs 3 arguments");
      }

      ExprEval x = args.get(0).eval(bindings);
      return x.asBoolean() ? args.get(1).eval(bindings) : args.get(2).eval(bindings);
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
      ExprType castTo;
      try {
        castTo = ExprType.valueOf(y.asString().toUpperCase());
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
        throw new IAE("function '%s' needs 1 or 2 arguments", name());
      }
      ExprEval value = args.get(0).eval(bindings);
      if (value.type() != ExprType.STRING) {
        throw new IAE("first argument should be string type but got %s type", value.type());
      }

      DateTimeFormatter formatter = ISODateTimeFormat.dateOptionalTimeParser();
      if (args.size() > 1) {
        ExprEval format = args.get(1).eval(bindings);
        if (format.type() != ExprType.STRING) {
          throw new IAE("second argument should be string type but got %s type", format.type());
        }
        formatter = DateTimeFormat.forPattern(format.asString());
      }
      DateTime date;
      try {
        date = DateTime.parse(value.asString(), formatter);
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
        throw new IAE("function 'nvl' needs 2 arguments");
      }
      final ExprEval eval = args.get(0).eval(bindings);
      return eval.isNull() ? args.get(1).eval(bindings) : eval;
    }
  }
}
