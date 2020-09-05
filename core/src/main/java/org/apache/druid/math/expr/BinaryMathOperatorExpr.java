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

package org.apache.druid.math.expr;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.apache.druid.common.config.NullHandling;

import javax.annotation.Nullable;

// math operators live here

class BinPlusExpr extends BinaryEvalOpExprBase
{
  BinPlusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinPlusExpr(op, left, right);
  }

  @Override
  protected ExprEval evalString(@Nullable String left, @Nullable String right)
  {
    return ExprEval.of(NullHandling.nullToEmptyIfNeeded(left)
                       + NullHandling.nullToEmptyIfNeeded(right));
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left + right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left + right;
  }
}

class BinMinusExpr extends BinaryEvalOpExprBase
{
  BinMinusExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinMinusExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left - right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left - right;
  }
}

class BinMulExpr extends BinaryEvalOpExprBase
{
  BinMulExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinMulExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left * right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left * right;
  }
}

class BinDivExpr extends BinaryEvalOpExprBase
{
  BinDivExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinDivExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left / right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left / right;
  }
}

class BinPowExpr extends BinaryEvalOpExprBase
{
  BinPowExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinPowExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return LongMath.pow(left, Ints.checkedCast(right));
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return Math.pow(left, right);
  }
}

class BinModuloExpr extends BinaryEvalOpExprBase
{
  BinModuloExpr(String op, Expr left, Expr right)
  {
    super(op, left, right);
  }

  @Override
  protected BinaryOpExprBase copy(Expr left, Expr right)
  {
    return new BinModuloExpr(op, left, right);
  }

  @Override
  protected final long evalLong(long left, long right)
  {
    return left % right;
  }

  @Override
  protected final double evalDouble(double left, double right)
  {
    return left % right;
  }
}
