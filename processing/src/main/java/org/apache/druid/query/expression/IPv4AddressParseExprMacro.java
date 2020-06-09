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

import javax.annotation.Nonnull;
import java.net.Inet4Address;
import java.util.List;

/**
 * <pre>
 * Implements an expression that parses a string or long into an IPv4 address stored (as an unsigned
 * int) in a long.
 *
 * Expression signatures:
 * - long ipv4_parse(string)
 * - long ipv4_parse(long)
 *
 * String arguments should be formatted as a dotted-decimal.
 * Long arguments that can be represented as an IPv4 address are passed through.
 * Invalid arguments return null.
 * </pre>
 *
 * @see IPv4AddressStringifyExprMacro
 * @see IPv4AddressMatchExprMacro
 */
public class IPv4AddressParseExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "ipv4_parse";

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

    Expr arg = args.get(0);

    class IPv4AddressParseExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private IPv4AddressParseExpr(Expr arg)
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
            return evalAsString(eval);
          case LONG:
            return evalAsLong(eval);
          default:
            return ExprEval.ofLong(null);
        }
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new IPv4AddressParseExpr(newArg));
      }
    }

    return new IPv4AddressParseExpr(arg);
  }

  private static ExprEval evalAsString(ExprEval eval)
  {
    Inet4Address address = IPv4AddressExprUtils.parse(eval.asString());
    Long value = address == null ? null : IPv4AddressExprUtils.toLong(address);
    return ExprEval.ofLong(value);
  }

  private static ExprEval evalAsLong(ExprEval eval)
  {
    if (eval.isNumericNull() || !IPv4AddressExprUtils.overflowsUnsignedInt(eval.asLong())) {
      return eval;
    }
    return ExprEval.ofLong(null);
  }
}
