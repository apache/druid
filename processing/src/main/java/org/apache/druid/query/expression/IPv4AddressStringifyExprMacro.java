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

import com.google.common.net.InetAddresses;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;

import javax.annotation.Nonnull;
import java.net.Inet4Address;
import java.util.List;

/**
 * <pre>
 * Implements an expression that converts an IPv4 address stored (as an unsigned int) in a long or
 * stored as a string into an IPv4 address dotted-decimal notated string (e.g., "192.168.0.1").
 *
 * Expression signatures:
 * - string ipv4address_stringify(long)
 * - string ipv4address_stringify(string)
 *
 * Valid argument formats are:
 * - unsigned int long (e.g., 3232235521)
 * - unsigned int string (e.g., "3232235521")
 * - IPv4 address dotted-decimal notation string (e.g., "198.168.0.1")
 * - IPv6 IPv4-mapped address string (e.g., "::ffff:192.168.0.1")
 *
 * Invalid arguments return null.
 *
 * The overloaded signature allows applying the expression to a dimension with mixed string and long
 * representations of IPv4 addresses.
 * </pre>
 *
 * @see IPv4AddressParseExprMacro
 * @see IPv4AddressMatchExprMacro
 */
public class IPv4AddressStringifyExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "ipv4address_stringify";

  @Override
  public String name()
  {
    return NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() != 1) {
      throw new IAE("Function[%s] must have 1 argument", name());
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
    String stringValue = eval.asString();
    if (stringValue == null) {
      return eval;
    }

    // Assume use cases in order of most frequent to least:
    // 1) convert long to string
    // 2) convert string to string

    try {
      long longValue = Long.parseLong(stringValue);
      return evalLong(longValue);
    }
    catch (NumberFormatException ignored) {
      // fall through
    }

    return ExprEval.of(IPv4AddressExprUtils.extractIPv4Address(stringValue));
  }

  private static ExprEval evalAsLong(ExprEval eval)
  {
    if (eval.isNumericNull()) {
      return eval;
    }

    return evalLong(eval.asLong());
  }

  private static ExprEval evalLong(long longValue)
  {
    if (IPv4AddressExprUtils.overflowsUnsignedInt(longValue)) {
      return ExprEval.of(null);
    }

    Inet4Address address = InetAddresses.fromInteger((int) longValue);
    return ExprEval.of(address.getHostAddress());
  }
}
