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

import inet.ipaddr.ipv6.IPv6Address;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
 
import javax.annotation.Nonnull;
import java.util.List;
 
/**
  * <pre>
  * Implements an expression that parses a string into an IPv6 address
  *
  * Expression signatures:
  * - long ipv6_parse(string)
  *
  * String arguments should be formatted as an IPv6 string e.g. "2001:4860:4860::8888"
  * Invalid arguments return null.
  * </pre>
  *
  * @see IPv6AddressStringifyExprMacro
  * @see IPv6AddressMatchExprMacro
  */
public class IPv6AddressParseExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "ipv6_parse";
 
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

    class IPv6AddressParseExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private IPv6AddressParseExpr(Expr arg)
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
    }
 
    return new IPv6AddressParseExpr(arg);
  }
 
  private static ExprEval evalAsString(ExprEval eval)
  {
    IPv6Address address = IPv6AddressExprUtils.parse(eval.asString());
    String value = address == null ? null : IPv6AddressExprUtils.toString(address);
    return ExprEval.of(value);
  }
}
