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
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchHolder;

import javax.annotation.Nullable;
import java.util.List;

public class HllPostAggExprMacros
{
  public static final String HLL_SKETCH_ESTIMATE = "hll_sketch_estimate";

  public static class HLLSketchEstimateExprMacro implements ExprMacroTable.ExprMacro
  {

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckAnyOfArgumentCount(args, 1, 2);
      return new HllSketchEstimateExpr(this, args);
    }

    @Override
    public String name()
    {
      return HLL_SKETCH_ESTIMATE;
    }
  }

  public static class HllSketchEstimateExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    private Expr estimateExpr;
    private Expr isRound;

    public HllSketchEstimateExpr(HLLSketchEstimateExprMacro macro, List<Expr> args)
    {
      super(macro, args);
      this.estimateExpr = args.get(0);
      if (args.size() == 2) {
        isRound = args.get(1);
      }
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return ExpressionType.DOUBLE;
    }

    @Override
    public ExprEval eval(ObjectBinding bindings)
    {
      boolean round = false;
      ExprEval eval = estimateExpr.eval(bindings);
      if (isRound != null) {
        round = isRound.eval(bindings).asBoolean();
      }

      final Object valObj = eval.value();
      if (valObj == null) {
        return ExprEval.of(0.0D);
      }
      HllSketchHolder h = HllSketchHolder.fromObj(valObj);
      double estimate = h.getEstimate();
      return round ? ExprEval.of(Math.round(estimate)) : ExprEval.of(estimate);
    }
  }
}

