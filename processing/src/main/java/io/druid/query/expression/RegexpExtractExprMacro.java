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

package io.druid.query.expression;

import io.druid.common.config.NullHandling;
import io.druid.java.util.common.IAE;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprMacroTable;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexpExtractExprMacro implements ExprMacroTable.ExprMacro
{
  @Override
  public String name()
  {
    return "regexp_extract";
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 2 || args.size() > 3) {
      throw new IAE("Function[%s] must have 2 to 3 arguments", name());
    }

    final Expr arg = args.get(0);
    final Expr patternExpr = args.get(1);
    final Expr indexExpr = args.size() > 2 ? args.get(2) : null;

    if (!patternExpr.isLiteral() || (indexExpr != null && !indexExpr.isLiteral())) {
      throw new IAE("Function[%s] pattern and index must be literals", name());
    }

    // Precompile the pattern.
    final Pattern pattern = Pattern.compile(String.valueOf(patternExpr.getLiteralValue()));

    final int index = indexExpr == null ? 0 : ((Number) indexExpr.getLiteralValue()).intValue();
    class RegexpExtractExpr implements Expr
    {
      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        String s = arg.eval(bindings).asString();
        final Matcher matcher = pattern.matcher(NullHandling.nullToEmptyIfNeeded(s));
        final String retVal = matcher.find() ? matcher.group(index) : null;
        return ExprEval.of(NullHandling.emptyToNullIfNeeded(retVal));
      }

      @Override
      public void visit(final Visitor visitor)
      {
        arg.visit(visitor);
        visitor.visit(this);
      }
    }
    return new RegexpExtractExpr();
  }
}
