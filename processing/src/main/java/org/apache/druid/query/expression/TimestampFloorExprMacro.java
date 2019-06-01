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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

  public static class TimestampFloorExpr implements Expr
  {
    private final Expr arg;
    private final PeriodGranularity granularity;

    public TimestampFloorExpr(final List<Expr> args)
    {
      this.arg = args.get(0);
      this.granularity = computeGranularity(args, ExprUtils.nilBindings());
    }

    /**
     * Exposed for Druid SQL: this is used by Expressions.toQueryGranularity.
     */
    public Expr getArg()
    {
      return arg;
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
      ExprEval eval = arg.eval(bindings);
      if (eval.isNumericNull()) {
        // Return null if the argument if null.
        return ExprEval.of(null);
      }
      return ExprEval.of(granularity.bucketStart(DateTimes.utc(eval.asLong())).getMillis());
    }

    @Override
    public void visit(final Visitor visitor)
    {
      arg.visit(visitor);
      visitor.visit(this);
    }


    @Override
    public Expr visit(Shuttle shuttle)
    {
      Expr newArg = arg.visit(shuttle);
      return shuttle.visit(new TimestampFloorExpr(ImmutableList.of(newArg)));
    }

    @Override
    public BindingDetails analyzeInputs()
    {
      final String identifier = arg.getIdentifierIfIdentifier();
      if (identifier == null) {
        return arg.analyzeInputs();
      }
      return arg.analyzeInputs().mergeWithScalars(ImmutableSet.of(identifier));
    }
  }

  public static class TimestampFloorDynamicExpr implements Expr
  {
    private final List<Expr> args;

    public TimestampFloorDynamicExpr(final List<Expr> args)
    {
      this.args = args;
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      final PeriodGranularity granularity = computeGranularity(args, bindings);
      return ExprEval.of(granularity.bucketStart(DateTimes.utc(args.get(0).eval(bindings).asLong())).getMillis());
    }

    @Override
    public void visit(final Visitor visitor)
    {
      for (Expr arg : args) {
        arg.visit(visitor);
      }
      visitor.visit(this);
    }

    @Override
    public Expr visit(Shuttle shuttle)
    {
      List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
      return shuttle.visit(new TimestampFloorDynamicExpr(newArgs));
    }

    @Override
    public BindingDetails analyzeInputs()
    {
      Set<String> scalars = new HashSet<>();
      BindingDetails accumulator = new BindingDetails();
      for (Expr arg : args) {
        final String identifier = arg.getIdentifierIfIdentifier();
        if (identifier != null) {
          scalars.add(identifier);
        }
        accumulator = accumulator.merge(arg.analyzeInputs());
      }
      return accumulator.mergeWithScalars(scalars);
    }
  }
}
