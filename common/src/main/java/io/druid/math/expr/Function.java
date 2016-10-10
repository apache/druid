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

import java.util.List;

/**
 */
interface Function
{
  String name();

  Number apply(List<Expr> args, Expr.ObjectBinding bindings);

  abstract class SingleParam implements Function
  {
    @Override
    public Number apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 1) {
        throw new RuntimeException("function '" + name() + "' needs 1 argument");
      }
      Expr expr = args.get(0);
      return eval(expr.eval(bindings));
    }

    protected abstract Number eval(Number x);
  }

  abstract class DoubleParam implements Function
  {
    @Override
    public Number apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 2) {
        throw new RuntimeException("function '" + name() + "' needs 1 argument");
      }
      Expr expr1 = args.get(0);
      Expr expr2 = args.get(1);
      return eval(expr1.eval(bindings), expr2.eval(bindings));
    }

    protected abstract Number eval(Number x, Number y);
  }

  class Abs extends SingleParam
  {
    @Override
    public String name()
    {
      return "abs";
    }

    @Override
    protected Number eval(Number x)
    {
      return x instanceof Long ? Math.abs(x.longValue()) : Math.abs(x.doubleValue());
    }
  }

  class Acos extends SingleParam
  {
    @Override
    public String name()
    {
      return "acos";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.acos(x.doubleValue());
    }
  }

  class Asin extends SingleParam
  {
    @Override
    public String name()
    {
      return "asin";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.asin(x.doubleValue());
    }
  }

  class Atan extends SingleParam
  {
    @Override
    public String name()
    {
      return "atan";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.atan(x.doubleValue());
    }
  }

  class Cbrt extends SingleParam
  {
    @Override
    public String name()
    {
      return "cbrt";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.cbrt(x.doubleValue());
    }
  }

  class Ceil extends SingleParam
  {
    @Override
    public String name()
    {
      return "ceil";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.ceil(x.doubleValue());
    }
  }

  class Cos extends SingleParam
  {
    @Override
    public String name()
    {
      return "cos";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.cos(x.doubleValue());
    }
  }

  class Cosh extends SingleParam
  {
    @Override
    public String name()
    {
      return "cosh";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.cosh(x.doubleValue());
    }
  }

  class Exp extends SingleParam
  {
    @Override
    public String name()
    {
      return "exp";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.exp(x.doubleValue());
    }
  }

  class Expm1 extends SingleParam
  {
    @Override
    public String name()
    {
      return "expm1";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.expm1(x.doubleValue());
    }
  }

  class Floor extends SingleParam
  {
    @Override
    public String name()
    {
      return "floor";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.floor(x.doubleValue());
    }
  }

  class GetExponent extends SingleParam
  {
    @Override
    public String name()
    {
      return "getExponent";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.getExponent(x.doubleValue());
    }
  }

  class Log extends SingleParam
  {
    @Override
    public String name()
    {
      return "log";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.log(x.doubleValue());
    }
  }

  class Log10 extends SingleParam
  {
    @Override
    public String name()
    {
      return "log10";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.log10(x.doubleValue());
    }
  }

  class Log1p extends SingleParam
  {
    @Override
    public String name()
    {
      return "log1p";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.log1p(x.doubleValue());
    }
  }

  class NextUp extends SingleParam
  {
    @Override
    public String name()
    {
      return "nextUp";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.nextUp(x.doubleValue());
    }
  }

  class Rint extends SingleParam
  {
    @Override
    public String name()
    {
      return "rint";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.rint(x.doubleValue());
    }
  }

  class Round extends SingleParam
  {
    @Override
    public String name()
    {
      return "round";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.round(x.doubleValue());
    }
  }

  class Signum extends SingleParam
  {
    @Override
    public String name()
    {
      return "signum";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.signum(x.doubleValue());
    }
  }

  class Sin extends SingleParam
  {
    @Override
    public String name()
    {
      return "sin";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.sin(x.doubleValue());
    }
  }

  class Sinh extends SingleParam
  {
    @Override
    public String name()
    {
      return "sinh";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.sinh(x.doubleValue());
    }
  }

  class Sqrt extends SingleParam
  {
    @Override
    public String name()
    {
      return "sqrt";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.sqrt(x.doubleValue());
    }
  }

  class Tan extends SingleParam
  {
    @Override
    public String name()
    {
      return "tan";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.tan(x.doubleValue());
    }
  }

  class Tanh extends SingleParam
  {
    @Override
    public String name()
    {
      return "tanh";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.tanh(x.doubleValue());
    }
  }

  class ToDegrees extends SingleParam
  {
    @Override
    public String name()
    {
      return "toDegrees";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.toDegrees(x.doubleValue());
    }
  }

  class ToRadians extends SingleParam
  {
    @Override
    public String name()
    {
      return "toRadians";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.toRadians(x.doubleValue());
    }
  }

  class Ulp extends SingleParam
  {
    @Override
    public String name()
    {
      return "ulp";
    }

    @Override
    protected Number eval(Number x)
    {
      return Math.ulp(x.doubleValue());
    }
  }

  class Atan2 extends DoubleParam
  {
    @Override
    public String name()
    {
      return "atan2";
    }

    @Override
    protected Number eval(Number x, Number y)
    {
      return Math.atan2(x.doubleValue(), y.doubleValue());
    }
  }

  class CopySign extends DoubleParam
  {
    @Override
    public String name()
    {
      return "copySign";
    }

    @Override
    protected Number eval(Number x, Number y)
    {
      return Math.copySign(x.doubleValue(), y.doubleValue());
    }
  }

  class Hypot extends DoubleParam
  {
    @Override
    public String name()
    {
      return "hypot";
    }

    @Override
    protected Number eval(Number x, Number y)
    {
      return Math.hypot(x.doubleValue(), y.doubleValue());
    }
  }

  class Remainder extends DoubleParam
  {
    @Override
    public String name()
    {
      return "remainder";
    }

    @Override
    protected Number eval(Number x, Number y)
    {
      return Math.IEEEremainder(x.doubleValue(), y.doubleValue());
    }
  }

  class Max extends DoubleParam
  {
    @Override
    public String name()
    {
      return "max";
    }

    @Override
    protected Number eval(Number x, Number y)
    {
      if (x instanceof Long && y instanceof Long) {
        return Math.max(x.longValue(), y.longValue());
      }
      return Double.compare(x.doubleValue(), y.doubleValue()) >= 0 ? x : y;
    }
  }

  class Min extends DoubleParam
  {
    @Override
    public String name()
    {
      return "min";
    }

    @Override
    protected Number eval(Number x, Number y)
    {
      if (x instanceof Long && y instanceof Long) {
        return Math.min(x.longValue(), y.longValue());
      }
      return Double.compare(x.doubleValue(), y.doubleValue()) <= 0 ? x : y;
    }
  }

  class NextAfter extends DoubleParam
  {
    @Override
    public String name()
    {
      return "nextAfter";
    }

    @Override
    protected Number eval(Number x, Number y)
    {
      return Math.nextAfter(x.doubleValue(), y.doubleValue());
    }
  }

  class Pow extends DoubleParam
  {
    @Override
    public String name()
    {
      return "pow";
    }

    @Override
    protected Number eval(Number x, Number y)
    {
      return Math.pow(x.doubleValue(), y.doubleValue());
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
    protected Number eval(Number x, Number y)
    {
      return Math.scalb(x.doubleValue(), y.intValue());
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
    public Number apply(List<Expr> args, Expr.ObjectBinding bindings)
    {
      if (args.size() != 3) {
        throw new RuntimeException("function 'if' needs 3 argument");
      }

      Number x = args.get(0).eval(bindings);
      if (x instanceof Long) {
        return x.longValue() > 0 ? args.get(1).eval(bindings) : args.get(2).eval(bindings);
      } else {
        return x.doubleValue() > 0 ? args.get(1).eval(bindings) : args.get(2).eval(bindings);
      }
    }
  }
}
