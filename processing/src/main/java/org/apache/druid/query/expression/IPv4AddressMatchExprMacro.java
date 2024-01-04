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

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
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
 * Implements an expression that checks if an IPv4 address belongs to a particular subnet.
 *
 * Expression signatures:
 * - long ipv4_match(string address, string subnet)
 * - long ipv4_match(long address, string subnet)
 *
 * Valid "address" argument formats are:
 * - unsigned int long (e.g., 3232235521)
 * - IPv4 address dotted-decimal string (e.g., "198.168.0.1")
 *
 * The argument format for the "subnet" argument should be a literal in CIDR notation
 * (e.g., "198.168.0.0/16").
 *
 * If the "address" argument does not represent an IPv4 address then false is returned.
 * </pre>
 *
 * @see IPv4AddressParseExprMacro
 * @see IPv4AddressStringifyExprMacro
 */
public class IPv4AddressMatchExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "ipv4_match";
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
      // we use 'blockString' for string matching with 'prefixContains' because we parse them into IPAddressString
      // for longs, we convert into a prefix block use 'block' and 'contains' so avoid converting to IPAddressString
      final IPAddressString blockString = getSubnetInfo(args);
      final IPAddress block = blockString.toAddress().toPrefixBlock();

      class IPv4AddressMatchExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {
        private IPv4AddressMatchExpr(List<Expr> args)
        {
          super(IPv4AddressMatchExprMacro.this, args);
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
            case LONG:
              match = !eval.isNumericNull() && isLongMatch(eval.asLong());
              break;
            default:
              match = false;
          }
          return ExprEval.ofLongBoolean(match);
        }

        private boolean isStringMatch(String stringValue)
        {
          IPAddressString addressString = IPv4AddressExprUtils.parseString(stringValue);
          return addressString != null && blockString.prefixContains(addressString);
        }

        private boolean isLongMatch(long longValue)
        {
          IPv4Address address = IPv4AddressExprUtils.parse(longValue);
          return address != null && block.contains(address);
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector)
        {
          return ExpressionType.LONG;
        }
      }

      return new IPv4AddressMatchExpr(args);
    }

    catch (AddressStringException e) {
      throw processingFailed(e, "failed to parse address");
    }
  }

  private IPAddressString getSubnetInfo(List<Expr> args)
  {
    String subnetArgName = "subnet";
    Expr arg = args.get(ARG_SUBNET);
    validationHelperCheckArgIsLiteral(arg, subnetArgName);
    String subnet = (String) arg.getLiteralValue();
    if (!IPv4AddressExprUtils.isValidIPv4Subnet(subnet)) {
      throw validationFailed(subnetArgName + " arg has an invalid format: " + subnet);
    }
    return new IPAddressString(subnet);
  }
}
