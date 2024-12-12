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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Computes a particular quantile from a numeric array.
 *
 * Usage: {@code array_quantile(array, rank)}. The requested quantile is given by "rank", which must be from 0 to 1,
 * inclusive: 0 is the minimum, 1 is the maximum. Null values in the input array are ignored.
 *
 * Returns {@link Double#NaN} if the requested quantile is below 0 or above 1. Returns {@link Double#NaN} if the
 * input array is numeric, yet contains no nonnull elements. Returns null if the input is not a numeric array at all.
 *
 * If the requested quantile falls between two elements of the input array, the result is a linear interpolation of
 * the two closest values. According to Wikipedia (https://en.wikipedia.org/wiki/Quantile), the interpolation algorithm
 * we're using is the default method in R, NumPy, and Julia, and matches Excel's PERCENTILE.INC function.
 */
public class ArrayQuantileExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "array_quantile";
  private static final String RANK_ARG_NAME = "rank";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentCount(args, 2);

    final Expr arg = args.get(0);
    final Expr rankArg = args.get(1);

    validationHelperCheckArgIsLiteral(rankArg, RANK_ARG_NAME);
    if (!(rankArg.getLiteralValue() instanceof Number)) {
      throw validationFailed("%s must be a number", RANK_ARG_NAME);
    }

    final double rank = ((Number) rankArg.getLiteralValue()).doubleValue();

    class ArrayQuantileExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
    {
      private ArrayQuantileExpr(List<Expr> args)
      {
        super(ArrayQuantileExprMacro.this, args);
      }

      @Nonnull
      @Override
      public ExprEval<?> eval(final ObjectBinding bindings)
      {
        final DoubleList doubles = toDoubleArray(arg.eval(bindings));

        if (doubles == null) {
          return ExprEval.ofDouble(null);
        }

        // Could speed up by using selection (like quickselect) instead of sort: expected O(n) instead of O(n logn).
        doubles.sort(null);
        return ExprEval.ofDouble(quantileFromSortedArray(doubles, rank));
      }

      @Nullable
      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return ExpressionType.DOUBLE;
      }
    }

    return new ArrayQuantileExpr(args);
  }

  /**
   * Returns a double[] copy of an {@link ExprEval}, or null if the eval is null, or is an array that contains any
   * nonnumeric elements. Nulls are skipped.
   */
  @Nullable
  static DoubleList toDoubleArray(final ExprEval<?> eval)
  {
    if (!eval.type().isArray() || !eval.type().getElementType().isNumeric()) {
      return null;
    }

    final Object[] arr = eval.asArray();

    if (arr == null) {
      return null;
    }

    // Copy array to double[], while verifying all elements are numbers and skipping nulls.
    final DoubleArrayList doubles = new DoubleArrayList(arr.length);

    for (final Object o : arr) {
      if (o != null) {
        doubles.add(((Number) o).doubleValue());
      }
    }

    return doubles;
  }

  static double quantileFromSortedArray(final DoubleList sortedDoubles, final double rank)
  {
    if (sortedDoubles.size() == 0 || rank < 0 || rank > 1) {
      return Double.NaN;
    }

    final double index = rank * (sortedDoubles.size() - 1);

    if (index <= 0) {
      // Minimum
      return sortedDoubles.getDouble(0);
    } else if (index >= sortedDoubles.size() - 1) {
      // Maximum
      return sortedDoubles.getDouble(sortedDoubles.size() - 1);
    } else if (index == (int) index) {
      // Specific element
      return sortedDoubles.getDouble((int) index);
    } else {
      // Linearly interpolate between two closest elements
      final double step = index - (int) index;
      final double a = sortedDoubles.getDouble((int) index);
      final double b = sortedDoubles.getDouble((int) index + 1);
      return a + step * (b - a);
    }
  }
}
