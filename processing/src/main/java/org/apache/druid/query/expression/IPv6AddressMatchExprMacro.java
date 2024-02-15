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

import inet.ipaddr.IPAddressString;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * <pre>
 * Implements an expression that checks if an IPv6 address belongs to a subnet.
 *
 * Expression signatures:
 * - long ipv6_match(string address, string subnet)
 *
 * Valid "address" argument formats are:
 * - IPv6 address string (e.g., "2001:4860:4860::8888")
 *
 * The argument format for the "subnet" argument should be a literal in CIDR notation
 * (e.g., "2001:db8::/64 ").
 *
 * If the "address" argument does not represent an IPv6 address then false is returned.
 * </pre>
 */
public class IPv6AddressMatchExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "ipv6_match";
  private static final int ARG_SUBNET = 1;

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 2);

    try {
      final Expr arg = args.get(0);
      final IPAddressString blockString = getSubnetInfo(args);

      class IPv6AddressMatchExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        private IPv6AddressMatchExpr(List<Expr> args)
        {
          super(IPv6AddressMatchExprMacro.this, args);
        }

        @Nonnull
        @Override
        public ExprEval eval(final ObjectBinding bindings)
        {
          ExprEval eval = arg.eval(bindings);
          boolean match;
          switch (eval.type().getType()) {
            case STRING:
              match = isStringMatch(eval.asString());
              break;
            default:
              match = false;
          }
          return ExprEval.ofLongBoolean(match);
        }

        private boolean isStringMatch(String stringValue)
        {
          IPAddressString addressString = IPv6AddressExprUtils.parseString(stringValue);
          return addressString != null && blockString.prefixContains(addressString);
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.LONG;
        }
      }

      return new IPv6AddressMatchExpr(args);
    }
    catch (Exception e) {
      throw processingFailed(e, "failed to parse address");
    }
  }

  private IPAddressString getSubnetInfo(List<Expr> args)
  {
    String subnetArgName = "subnet";
    Expr arg = args.get(ARG_SUBNET);
    validationHelperCheckArgIsLiteral(arg, subnetArgName);
    String subnet = (String) arg.getLiteralValue();
    if (!IPv6AddressExprUtils.isValidIPv6Subnet(subnet)) {
      throw validationFailed(subnetArgName + " arg has an invalid format: " + subnet);
    }
    return new IPAddressString(subnet);
  }
}
