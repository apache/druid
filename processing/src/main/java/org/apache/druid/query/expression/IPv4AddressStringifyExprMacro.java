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
 * Implements an expression that converts a long or a string into an IPv4 address dotted-decimal string.
 *
 * Expression signatures:
 * - string ipv4_stringify(long)
 * - string ipv4_stringify(string)
 *
 * Long arguments that can be represented as an IPv4 address are converted to a dotted-decimal string.
 * String arguments that are dotted-decimal IPv4 addresses are passed through.
 * Invalid arguments return null.
 * </pre>
 *
 * @see IPv4AddressParseExprMacro
 * @see IPv4AddressMatchExprMacro
 */
public class IPv4AddressStringifyExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "ipv4_stringify";

  @Override
  public String name()
  {
    return NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() != 1) {
      throw new IAE(ExprUtils.createErrMsg(name(), "must have 1 argument"));
    }

    Expr arg = args.get(0);

    class IPv4AddressStringifyExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private IPv4AddressStringifyExpr(Expr arg)
      {
        super(arg);
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
            return ExprEval.of(null);
        }
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new IPv4AddressStringifyExpr(newArg));
      }
    }

    return new IPv4AddressStringifyExpr(arg);
  }

  private static ExprEval evalAsString(ExprEval eval)
  {
    if (IPv4AddressExprUtils.isValidAddress(eval.asString())) {
      return eval;
    }
    return ExprEval.of(null);
  }

  private static ExprEval evalAsLong(ExprEval eval)
  {
    if (eval.isNumericNull()) {
      return ExprEval.of(null);
    }

    long longValue = eval.asLong();
    if (IPv4AddressExprUtils.overflowsUnsignedInt(longValue)) {
      return ExprEval.of(null);
    }

    Inet4Address address = IPv4AddressExprUtils.parse((int) longValue);
    return ExprEval.of(IPv4AddressExprUtils.toString(address));
  }
}
