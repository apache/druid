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

package org.apache.druid.query.aggregation.datasketches.theta.sql;

import com.google.common.collect.Iterables;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;

import javax.annotation.Nullable;
import java.util.List;

public class ThetaPostAggMacros
{
  public static final String THETA_SKETCH_ESTIMATE = "theta_sketch_estimate";

  public static class ThetaSketchEstimateExprMacro implements ExprMacroTable.ExprMacro
  {

    @Override
    public Expr apply(List<Expr> args)
    {
      validationHelperCheckArgumentCount(args, 1);
      return new ThetaSketchEstimateExpr(this, args);
    }

    @Override
    public String name()
    {
      return THETA_SKETCH_ESTIMATE;
    }
  }

  public static class ThetaSketchEstimateExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    private Expr estimateExpr;

    public ThetaSketchEstimateExpr(ThetaSketchEstimateExprMacro macro, List<Expr> args)
    {
      super(macro, args);
      this.estimateExpr = Iterables.getOnlyElement(args);
    }

    @Override
    public ExprEval eval(ObjectBinding bindings)
    {
      ExprEval eval = estimateExpr.eval(bindings);
      final Object valObj = eval.value();
      if (valObj == null) {
        return ExprEval.of(null);
      }
      if (valObj instanceof SketchHolder) {
        SketchHolder thetaSketchHolder = (SketchHolder) valObj;
        double estimate = thetaSketchHolder.getEstimate();
        return ExprEval.of(estimate);
      } else {
        throw new IllegalArgumentException("requires a ThetaSketch as the argument");
      }
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return ExpressionType.DOUBLE;
    }
  }
}
