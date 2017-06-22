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

package io.druid.query.expression;

import io.druid.java.util.common.IAE;
import io.druid.java.util.common.granularity.PeriodGranularity;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprMacroTable;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import java.util.List;

public class TimestampCeilExprMacro implements ExprMacroTable.ExprMacro
{
  @Override
  public String name()
  {
    return "timestamp_ceil";
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 2 || args.size() > 4) {
      throw new IAE("'%s' must have 2 to 4 arguments", name());
    }

    final Expr arg = args.get(0);

    if (args.stream().skip(1).allMatch(Expr::isLiteral)) {
      final PeriodGranularity granularity = getGranularity(args, ExprUtils.nilBindings());

      class TimestampCeilExpr implements Expr
      {
        @Nonnull
        @Override
        public ExprEval eval(final ObjectBinding bindings)
        {
          return ExprEval.of(granularity.bucketEnd(new DateTime(arg.eval(bindings).asLong())).getMillis());
        }

        @Override
        public void visit(final Visitor visitor)
        {
          arg.visit(visitor);
          visitor.visit(this);
        }
      }
      return new TimestampCeilExpr();
    } else {
      class TimestampCeilDynamicExpr implements Expr
      {
        @Nonnull
        @Override
        public ExprEval eval(final ObjectBinding bindings)
        {
          final PeriodGranularity granularity = getGranularity(args, bindings);
          return ExprEval.of(granularity.bucketEnd(new DateTime(arg.eval(bindings).asLong())).getMillis());
        }

        @Override
        public void visit(final Visitor visitor)
        {
          arg.visit(visitor);
          visitor.visit(this);
        }
      }
      return new TimestampCeilDynamicExpr();
    }
  }

  private static PeriodGranularity getGranularity(final List<Expr> args, final Expr.ObjectBinding bindings)
  {
    return ExprUtils.toPeriodGranularity(
        args.get(1),
        args.size() > 2 ? args.get(2) : null,
        args.size() > 3 ? args.get(3) : null,
        bindings
    );
  }
}
