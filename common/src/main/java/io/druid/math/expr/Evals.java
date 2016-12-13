/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.math.expr;

import com.google.common.base.Strings;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.logger.Logger;

import java.util.Arrays;
import java.util.List;

/**
 */
public class Evals
{
  private static final Logger log = new Logger(Evals.class);

  public static Number toNumber(Object value)
  {
    if (value == null) {
      return 0L;
    }
    if (value instanceof Number) {
      return (Number) value;
    }
    String stringValue = String.valueOf(value);
    Long longValue = GuavaUtils.tryParseLong(stringValue);
    if (longValue == null) {
      return Double.valueOf(stringValue);
    }
    return longValue;
  }

  public static boolean isConstant(Expr expr)
  {
    return expr instanceof ConstantExpr;
  }

  public static boolean isAllConstants(Expr... exprs)
  {
    return isAllConstants(Arrays.asList(exprs));
  }

  public static boolean isAllConstants(List<Expr> exprs)
  {
    for (Expr expr : exprs) {
      if (!(expr instanceof ConstantExpr)) {
        return false;
      }
    }
    return true;
  }

  // for binary operator not providing constructor of form <init>(String, Expr, Expr),
  // you should create it explicitly in here
  public static Expr binaryOp(BinaryOpExprBase binary, Expr left, Expr right)
  {
    try {
      return binary.getClass()
                   .getDeclaredConstructor(String.class, Expr.class, Expr.class)
                   .newInstance(binary.op, left, right);
    }
    catch (Exception e) {
      log.warn(e, "failed to rewrite expression " + binary);
      return binary;  // best effort.. keep it working
    }
  }

  public static long asLong(boolean x)
  {
    return x ? 1L : 0L;
  }

  public static double asDouble(boolean x)
  {
    return x ? 1D : 0D;
  }

  public static String asString(boolean x)
  {
    return String.valueOf(x);
  }

  public static boolean asBoolean(long x)
  {
    return x > 0;
  }

  public static boolean asBoolean(double x)
  {
    return x > 0;
  }

  public static boolean asBoolean(String x)
  {
    return !Strings.isNullOrEmpty(x) && Boolean.valueOf(x);
  }
}
