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

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.DimensionHandlerUtils;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

public class RandExprMacro implements ExprMacroTable.ExprMacro
{
  @Override
  public String name()
  {
    return "rand";
  }

  @Override
  public Expr apply(List<Expr> args)
  {
    final DoubleSupplier randomGenerator;

    if (args.isEmpty()) {
      randomGenerator = () -> ThreadLocalRandom.current().nextDouble();
    } else if (args.size() == 1) {
      final Expr seedArg = Iterables.getOnlyElement(args);
      if (seedArg.isLiteral()) {
        final Long seedValue = DimensionHandlerUtils.convertObjectToLong(seedArg.getLiteralValue());
        if (seedValue != null) {
          final Random random = new Random(seedValue);
          randomGenerator = random::nextDouble;
        } else {
          throw new IAE("Function[%s] first argument must be a number literal");
        }
      } else {
        throw new IAE("Function[%s] first argument must be a number literal");
      }
    } else {
      throw new IAE("Function[%s] needs zero or one arguments", name());
    }

    class RandExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private RandExpr()
      {
        super(Collections.emptyList());
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        return ExprEval.of(randomGenerator.getAsDouble());
      }

      @Override
      public void visit(Visitor visitor)
      {
        visitor.visit(this);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(this);
      }
    }

    return new RandExpr();
  }
}
