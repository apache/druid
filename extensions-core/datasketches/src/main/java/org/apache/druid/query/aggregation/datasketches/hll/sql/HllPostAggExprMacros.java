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

package org.apache.druid.query.aggregation.datasketches.hll.sql;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchHolder;

import java.util.List;
import java.util.stream.Collectors;

public class HllPostAggExprMacros
{
  // make same name as SQL binding
  public static final String HLL_SKETCH_ESTIMATE = "HLL_SKETCH_ESTIMATE";

  public static class HLLSketchEstimateExprMacro implements ExprMacroTable.ExprMacro
  {

    @Override
    public Expr apply(List<Expr> args)
    {
      class HllSketchEstimateExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
      {

        public HllSketchEstimateExpr(List<Expr> args)
        {
          super(HLL_SKETCH_ESTIMATE, args);
        }

        @Override
        public ExprEval eval(ObjectBinding bindings)
        {
          boolean round = false;
          Expr arg = args.get(0);
          ExprEval eval = arg.eval(bindings);
          if (args.size() == 2) {
            round = args.get(1).eval(bindings).asBoolean();
          }

          final Object valObj = eval.value();
          if (valObj == null) {
            return ExprEval.of(null);
          }
          if (eval.type().is(ExprType.COMPLEX) && valObj instanceof HllSketchHolder) {
            HllSketchHolder h = HllSketchHolder.fromObj(valObj);
            double estimate = h.getEstimate();
            return round ? ExprEval.of(Math.round(estimate)) : ExprEval.of(estimate);
          } else {
            throw HLLSketchEstimateExprMacro.this.validationFailed("requires a HllSketch as the argument");
          }
        }

        @Override
        public Expr visit(Shuttle shuttle)
        {
          List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
          return shuttle.visit(new HllSketchEstimateExpr(newArgs));
        }
      }
      return new HllSketchEstimateExpr(args);
    }

    @Override
    public String name()
    {
      return HLL_SKETCH_ESTIMATE;
    }
  }
}
