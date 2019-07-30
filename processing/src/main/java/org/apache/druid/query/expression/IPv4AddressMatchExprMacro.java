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

import com.google.common.base.Preconditions;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * <pre>
 * Implements an expression that checks if an IPv4 address belongs to a particular subnet.
 *
 * Expression signatures:
 * - boolean ipv4address_match(string address, string subnet)
 * - boolean ipv4address_match(string address, string subnet, boolean inclusive)
 * - boolean ipv4address_match(long address, string subnet)
 * - boolean ipv4address_match(long address, string subnet, boolean inclusive)
 *
 * Valid "address" argument formats are:
 * - unsigned int long (e.g., 3232235521)
 * - unsigned int string (e.g., "3232235521")
 * - IPv4 address dotted-decimal notation string (e.g., "198.168.0.1")
 * - IPv6 IPv4-mapped address string (e.g., "::ffff:192.168.0.1")
 *
 * The argument format for the "subnet" argument should be a literal in CIDR notation
 * (e.g., "198.168.0.0/16").
 *
 * An optional "inclusive" argument should be a boolean literal (e.g., 1, 0, "true", or "false") that
 * indicates whether the network and the broadcast addresses should be considered part of the
 * subnet. When this argument is absent, its value defaults to "false".
 *
 * The overloaded signature allows applying the expression to a dimension with mixed string and long
 * representations of IPv4 addresses.
 * </pre>
 *
 * @see IPv4AddressParseExprMacro
 * @see IPv4AddressStringifyExprMacro
 */
public class IPv4AddressMatchExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String NAME = "ipv4address_match";
  private static final int ARG_SUBNET = 1;
  private static final int ARG_INCLUSIVE = 2;

  @Override
  public String name()
  {
    return NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() < 2 || 3 < args.size()) {
      throw new IAE(createErrMsg("must have 2-3 arguments"));
    }

    boolean inclusive = getInclusive(args);
    SubnetUtils.SubnetInfo subnetInfo = getSubnetInfo(args, inclusive);

    Expr arg = args.get(0);

    class IPv4AddressMatchExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private final SubnetUtils.SubnetInfo subnetInfo;

      private IPv4AddressMatchExpr(Expr arg, SubnetUtils.SubnetInfo subnetInfo)
      {
        super(arg);
        this.subnetInfo = subnetInfo;
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        ExprEval eval = arg.eval(bindings);
        boolean match;
        switch (eval.type()) {
          case STRING:
            match = isStringMatch(eval.asString());
            break;
          case LONG:
            match = !eval.isNumericNull() && isLongMatch(eval.asLong());
            break;
          default:
            match = false;
        }
        return ExprEval.of(match, ExprType.LONG);
      }

      private boolean isStringMatch(String stringValue)
      {
        boolean match;
        String ipv4 = IPv4AddressExprUtils.extractIPv4Address(stringValue);
        if (ipv4 == null) {
          match = isLongMatch(stringValue);
        } else {
          match = subnetInfo.isInRange(ipv4);
        }
        return match;
      }

      private boolean isLongMatch(String stringValue)
      {
        boolean match;
        try {
          long longValue = Long.parseLong(stringValue);
          match = isLongMatch(longValue);
        }
        catch (NumberFormatException ignored) {
          match = false;
        }
        return match;
      }

      private boolean isLongMatch(long longValue)
      {
        return !IPv4AddressExprUtils.overflowsUnsignedInt(longValue) && subnetInfo.isInRange((int) longValue);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new IPv4AddressMatchExpr(newArg, subnetInfo));
      }
    }

    return new IPv4AddressMatchExpr(arg, subnetInfo);
  }

  private String createErrMsg(String msg)
  {
    String prefix = "Function[" + name() + "] ";
    return prefix + msg;
  }

  private boolean getInclusive(List<Expr> args)
  {
    boolean inclusive = false;
    if (ARG_INCLUSIVE < args.size()) {
      Expr arg = args.get(ARG_INCLUSIVE);
      checkLiteralArgument(arg, "inclusive");
      ExprEval eval = arg.eval(ExprUtils.nilBindings());
      inclusive = eval.asBoolean();
    }
    return inclusive;
  }

  private void checkLiteralArgument(Expr arg, String name)
  {
    Preconditions.checkArgument(arg.isLiteral(), createErrMsg(name + " arg must be a literal"));
  }

  private SubnetUtils.SubnetInfo getSubnetInfo(List<Expr> args, boolean inclusive)
  {
    String subnetArgName = "subnet";
    Expr arg = args.get(ARG_SUBNET);
    checkLiteralArgument(arg, subnetArgName);
    String subnet = (String) arg.getLiteralValue();

    SubnetUtils subnetUtils;
    try {
      subnetUtils = new SubnetUtils(subnet);
    }
    catch (IllegalArgumentException e) {
      throw new IAE(e, createErrMsg(subnetArgName + " arg has an invalid format: " + subnet));
    }
    subnetUtils.setInclusiveHostCount(inclusive);

    return subnetUtils.getInfo();
  }
}
