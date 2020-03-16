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

import org.apache.commons.net.util.SubnetUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
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
    if (args.size() != 2) {
      throw new IAE(ExprUtils.createErrMsg(name(), "must have 2 arguments"));
    }

    SubnetUtils.SubnetInfo subnetInfo = getSubnetInfo(args);
    Expr arg = args.get(0);

    class IPv4AddressMatchExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private final SubnetUtils.SubnetInfo subnetInfo;

      private IPv4AddressMatchExpr(Expr arg, SubnetUtils.SubnetInfo subnetInfo)
      {
        super(FN_NAME, arg);
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
        return IPv4AddressExprUtils.isValidAddress(stringValue) && subnetInfo.isInRange(stringValue);
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

      @Override
      public String stringify()
      {
        return StringUtils.format("%s(%s, %s)", FN_NAME, arg.stringify(), args.get(ARG_SUBNET).stringify());
      }
    }

    return new IPv4AddressMatchExpr(arg, subnetInfo);
  }

  private SubnetUtils.SubnetInfo getSubnetInfo(List<Expr> args)
  {
    String subnetArgName = "subnet";
    Expr arg = args.get(ARG_SUBNET);
    ExprUtils.checkLiteralArgument(name(), arg, subnetArgName);
    String subnet = (String) arg.getLiteralValue();

    SubnetUtils subnetUtils;
    try {
      subnetUtils = new SubnetUtils(subnet);
    }
    catch (IllegalArgumentException e) {
      throw new IAE(e, ExprUtils.createErrMsg(name(), subnetArgName + " arg has an invalid format: " + subnet));
    }
    subnetUtils.setInclusiveHostCount(true);  // make network and broadcast addresses match

    return subnetUtils.getInfo();
  }
}
