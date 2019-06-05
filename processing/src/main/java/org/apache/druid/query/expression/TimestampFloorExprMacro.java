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

package org.apache.druid.query.expression;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

public class TimestampFloorExprMacro implements ExprMacroTable.ExprMacro
{
  @Override
  public String name()
  {
    return "timestamp_floor";
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 2 || args.size() > 4) {
      throw new IAE("Function[%s] must have 2 to 4 arguments", name());
    }

    if (args.stream().skip(1).allMatch(Expr::isLiteral)) {
      return new TimestampFloorExpr(args);
    } else {
      return new TimestampFloorDynamicExpr(args);
    }
  }

  private static PeriodGranularity computeGranularity(final List<Expr> args, final Expr.ObjectBinding bindings)
  {
    return ExprUtils.toPeriodGranularity(
        args.get(1),
        args.size() > 2 ? args.get(2) : null,
        args.size() > 3 ? args.get(3) : null,
        bindings
    );
  }

  public static class TimestampFloorExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    private final PeriodGranularity granularity;

    TimestampFloorExpr(final List<Expr> args)
    {
      super(args);
      this.granularity = computeGranularity(args, ExprUtils.nilBindings());
    }

    /**
     * Exposed for Druid SQL: this is used by Expressions.toQueryGranularity.
     */
    public Expr getArg()
    {
      return args.get(0);
    }

    /**
     * Exposed for Druid SQL: this is used by Expressions.toQueryGranularity.
     */
    public PeriodGranularity getGranularity()
    {
      return granularity;
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      ExprEval eval = args.get(0).eval(bindings);
      if (eval.isNumericNull()) {
        // Return null if the argument if null.
        return ExprEval.of(null);
      }
      return ExprEval.of(granularity.bucketStart(DateTimes.utc(eval.asLong())).getMillis());
    }

    @Override
    public Expr visit(Shuttle shuttle)
    {
      List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());

      return shuttle.visit(new TimestampFloorExpr(newArgs));
    }
  }

  public static class TimestampFloorDynamicExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    TimestampFloorDynamicExpr(final List<Expr> args)
    {
      super(args);
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      final PeriodGranularity granularity = computeGranularity(args, bindings);
      return ExprEval.of(granularity.bucketStart(DateTimes.utc(args.get(0).eval(bindings).asLong())).getMillis());
    }

    @Override
    public Expr visit(Shuttle shuttle)
    {
      List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
      return shuttle.visit(new TimestampFloorDynamicExpr(newArgs));
    }
  }
}
