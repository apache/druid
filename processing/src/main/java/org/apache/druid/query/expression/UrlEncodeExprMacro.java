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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class UrlEncodeExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "urlencode";

  private static class UrlEncodeExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
  {
    private UrlEncodeExpr(Expr arg)
    {
      super(FN_NAME, arg);
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      ExprEval eval = arg.eval(bindings);
      switch (eval.type()) {
        case STRING:
          return ExprEval.of(encode(eval.asString()));
        default:
          return ExprEval.of(null);
      }
    }

    @Override
    public Expr visit(Shuttle shuttle)
    {
      Expr newArg = arg.visit(shuttle);
      return shuttle.visit(new UrlEncodeExpr(newArg));
    }

    @Nullable
    @Override
    public ExprType getOutputType(InputBindingInspector inspector)
    {
      return ExprType.STRING;
    }
  }

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() != 1) {
      throw new IAE(ExprUtils.createErrMsg(name(), "must have 1 argument"));
    }

    return new UrlEncodeExpr(args.get(0));
  }

  private static String encode(final String s)
  {
    if (s == null) {
      return null;
    }

    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8.name());
    }
    catch (UnsupportedEncodingException e) {
      throw new RuntimeException("unable to encode to an application/x-www-form-urlencoded string", e);
    }
  }
}
