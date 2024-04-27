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

package org.apache.druid.query.expressions;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprMacroTable.ExprMacro;
import org.apache.druid.math.expr.ExpressionType;

import java.util.List;

/**
 * This function makes the current thread sleep for the given amount of seconds.
 * Fractional-second delays can be specified.
 *
 * This function is applied per row. The actual query time can vary depending on how much parallelism is used
 * for the query. As it does not provide consistent sleep time, this function should be used only for testing
 * when you want to keep a certain query running during the test.
 */
public class SleepExprMacro implements ExprMacro
{
  private static final String NAME = "sleep";

  @Override
  public String name()
  {
    return NAME;
  }

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 1);

    Expr arg = args.get(0);

    class SleepExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      public SleepExpr(List<Expr> args)
      {
        super(SleepExprMacro.this, args);
      }

      @Override
      public ExprEval eval(ObjectBinding bindings)
      {
        ExprEval eval = arg.eval(bindings);
        try {
          if (!eval.isNumericNull()) {
            double seconds = eval.asDouble(); // double to support fractional-second.
            if (seconds > 0) {
              Thread.sleep((long) (seconds * 1000));
            }
          }
          return ExprEval.of(null);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw processingFailed(e, "interrupted");
        }
      }

      /**
       * Explicitly override this method to not vectorize the sleep expression.
       * If we ever want to vectorize this expression, {@link #getOutputType} should be considered to return something
       * else than just null.
       */
      @Override
      public boolean canVectorize(InputBindingInspector inspector)
      {
        return false;
      }

      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return null;
      }
    }
    return new SleepExpr(args);
  }
}
