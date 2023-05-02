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

package org.apache.druid.query.aggregation.pair;

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;

import javax.annotation.Nullable;
import java.util.List;

public class PairRightExprMacro implements ExprMacroTable.ExprMacro
{
  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 1);
    Expr arg = args.get(0);

    class PairRightExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      public PairRightExpr(Expr arg)
      {
        super("pair_right", arg);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        Object pairResult = args.get(0).eval(bindings).value();
        if (pairResult instanceof SerializablePair){
          return ExprEval.bestEffortOf(((SerializablePair) pairResult).rhs);
        }

        return ExprEval.of(null);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(apply(shuttle.visitAll(args)));
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        final ExpressionType inputType = inspector.getType(arg.getBindingIfIdentifier());
        if (inputType.is(ExprType.COMPLEX)) {
          if (StringFirstAggregatorFactory.TYPE.getComplexTypeName().equals(inputType.getComplexTypeName())) {
            return ExpressionType.STRING;
          }
        }
        throw new UOE("Invalid input type [%s] to pair_right, expected a complex pair type");
      }
    }
    return new PairRightExpr(arg);
  }

  @Override
  public String name()
  {
    return "pair_right";
  }
}
