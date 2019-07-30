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
import javax.annotation.Nullable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.List;

/**
 * <pre>
 * Implements an expression that parses string or long into an IPv4 address stored (as an unsigned
 * int) in a long.
 *
 * Expression signatures:
 * - long ipv4address_parse(string)
 * - long ipv4address_parse(long)
 *
 * Valid argument formats are:
 * - IPv4 address dotted-decimal notation string (e.g., "198.168.0.1")
 * - IPv6 IPv4-mapped adress string (e.g., "::ffff:192.168.0.1")
 * - unsigned int long (e.g., 3232235521)
 * - unsigned int string (e.g., "3232235521")
 *
 * Invalid arguments return null.
 *
 * The overloaded signature allows applying the expression to a dimension with mixed string and long
 * representations of IPv4 addresses.
 * </pre>
 *
 * @see IPv4AddressStringifyExprMacro
 * @see IPv4AddressMatchExprMacro
 */
public class IPv4AddressParseExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "ipv4address_parse";

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

    class IPv4AddressParseExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private IPv4AddressParseExpr(Expr arg)
      {
        super(arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        String stringValue = arg.eval(bindings).asString();
        if (stringValue == null) {
          return ExprEval.ofLong(null);
        }

        // Assume use cases in order of most frequent to least are:
        // 1) convert string to long
        // 2) convert long to long
        Long value = parseAsString(stringValue);
        if (value == null) {
          value = parseAsLong(stringValue);
        }

        return ExprEval.ofLong(value);
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

  @Nullable
  private static Long parseAsString(String stringValue)
  {
    try {
      // Do not use java.lang.InetAddress#getByName() as it may do DNS lookups
      InetAddress address = InetAddresses.forString(stringValue);
      if (address instanceof Inet4Address) {
        int value = InetAddresses.coerceToInteger(address);
        return Integer.toUnsignedLong(value);
      }
    }
    catch (IllegalArgumentException ignored) {
      // fall through (Invalid IPv4 adddress string)
    }
    return null;
  }

  @Nullable
  private static Long parseAsLong(String stringValue)
  {
    try {
      Long value = Long.valueOf(stringValue);
      if (!IPv4AddressExprUtils.overflowsUnsignedInt(value)) {
        return value;
      }
    }
    catch (NumberFormatException ignored) {
      // fall through
    }
    return null;
  }
}
