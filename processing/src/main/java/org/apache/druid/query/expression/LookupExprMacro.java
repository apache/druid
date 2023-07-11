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

import com.google.inject.Inject;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Exprs;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.RegisteredLookupExtractionFn;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class LookupExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "lookup";
  private final LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider;

  @Inject
  public LookupExprMacro(final LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider)
  {
    this.lookupExtractorFactoryContainerProvider = lookupExtractorFactoryContainerProvider;
  }

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 2);

    final Expr arg = args.get(0);
    final Expr lookupExpr = args.get(1);

    validationHelperCheckArgIsLiteral(lookupExpr, "second argument");
    if (lookupExpr.getLiteralValue() == null) {
      throw validationFailed("second argument must be a registered lookup name");
    }

    final String lookupName = lookupExpr.getLiteralValue().toString();
    final RegisteredLookupExtractionFn extractionFn = new RegisteredLookupExtractionFn(
        lookupExtractorFactoryContainerProvider,
        lookupName,
        false,
        null,
        false,
        null
    );

    class LookupExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private LookupExpr(Expr arg)
      {
        super(FN_NAME, arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        return ExprEval.of(extractionFn.apply(NullHandling.emptyToNullIfNeeded(arg.eval(bindings).asString())));
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(apply(shuttle.visitAll(args)));
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.STRING;
      }

      @Override
      public String stringify()
      {
        return StringUtils.format("%s(%s, %s)", FN_NAME, arg.stringify(), lookupExpr.stringify());
      }

      @Override
      public byte[] getCacheKey()
      {
        return new CacheKeyBuilder(Exprs.LOOKUP_EXPR_CACHE_KEY).appendString(stringify())
                                                               .appendCacheable(extractionFn)
                                                               .build();
      }
    }

    return new LookupExpr(arg);
  }
}
