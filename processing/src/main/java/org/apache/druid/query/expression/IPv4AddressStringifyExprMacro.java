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

import inet.ipaddr.ipv4.IPv4Address;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
  public static final String FN_NAME = "ipv4_stringify";

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

    class IPv4AddressStringifyExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private IPv4AddressStringifyExpr(List<Expr> args)
      {
        super(IPv4AddressStringifyExprMacro.this, args);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        ExprEval eval = arg.eval(bindings);
        switch (eval.type().getType()) {
          case STRING:
            return evalAsString(eval);
          case LONG:
            return evalAsLong(eval);
          default:
            return ExprEval.of(null);
        }
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.STRING;
      }
    }

    return new IPv4AddressStringifyExpr(args);
  }

  private static ExprEval evalAsString(ExprEval eval)
  {
    if (IPv4AddressExprUtils.isValidIPv4Address(eval.asString())) {
      return eval;
    }
    return ExprEval.of(null);
  }

  private static ExprEval evalAsLong(ExprEval eval)
  {
    if (eval.isNumericNull()) {
      return ExprEval.of(null);
    }

    IPv4Address address = IPv4AddressExprUtils.parse(eval.asLong());
    if (address == null) {
      return ExprEval.of(null);
    }
    return ExprEval.of(IPv4AddressExprUtils.toString(address));
  }
}
