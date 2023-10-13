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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
 
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
 
 /**
  * <pre>
  * Implements an expression that converts a string into an IPv6 address dotted-decimal string.
  *
  * Expression signatures:
  * - string ipv6_stringify(string)
  *
  * String arguments that are dotted-decimal IPv6 addresses
  * Invalid arguments return null.
  * </pre>
  *
  * @see IPv6AddressParseExprMacro
  * @see IPv6AddressMatchExprMacro
  */
public class IPv6AddressStringifyExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "ipv6_stringify";

  @Override
  public String name()
  {
    return FN_NAME;
  }
 
  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 1);
 
    Expr arg = args.get(0);
 
    class IPv6AddressStringifyExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private IPv6AddressStringifyExpr(Expr arg)
      {
        super(FN_NAME, arg);
      }
 
      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        ExprEval eval = arg.eval(bindings);
        switch (eval.type().getType()) {
          case STRING:
            return evalAsString(eval);
          default:
            return ExprEval.of(null);
        }
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
    }

    return new IPv6AddressStringifyExpr(arg);
  }
 
  private static ExprEval evalAsString(ExprEval eval)
  {
    if (IPv6AddressExprUtils.isValidIPv6Address(eval.asString())) {
      return eval;
    }
    return ExprEval.of(null);
  }
}
