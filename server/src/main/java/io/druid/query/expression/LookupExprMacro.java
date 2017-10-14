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

import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.druid.java.util.common.IAE;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.lookup.LookupReferencesManager;
import io.druid.query.lookup.RegisteredLookupExtractionFn;

import javax.annotation.Nonnull;
import java.util.List;

public class LookupExprMacro implements ExprMacroTable.ExprMacro
{
  private final LookupReferencesManager lookupReferencesManager;

  @Inject
  public LookupExprMacro(final LookupReferencesManager lookupReferencesManager)
  {
    this.lookupReferencesManager = lookupReferencesManager;
  }

  @Override
  public String name()
  {
    return "lookup";
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() != 2) {
      throw new IAE("Function[%s] must have 2 arguments", name());
    }

    final Expr arg = args.get(0);
    final Expr lookupExpr = args.get(1);

    if (!lookupExpr.isLiteral() || lookupExpr.getLiteralValue() == null) {
      throw new IAE("Function[%s] second argument must be a registered lookup name", name());
    }

    final String lookupName = lookupExpr.getLiteralValue().toString();
    final RegisteredLookupExtractionFn extractionFn = new RegisteredLookupExtractionFn(
        lookupReferencesManager,
        lookupName,
        false,
        null,
        false,
        null
    );

    class LookupExpr implements Expr
    {
      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        return ExprEval.of(extractionFn.apply(Strings.emptyToNull(arg.eval(bindings).asString())));
      }

      @Override
      public void visit(final Visitor visitor)
      {
        arg.visit(visitor);
        visitor.visit(this);
      }
    }

    return new LookupExpr();
  }
}
