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

package org.apache.druid.segment.virtual;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Expression column value selector that examines a set of 'unknown' type input bindings on a row by row basis,
 * transforming the expression to handle multi-value list typed inputs as they are encountered.
 *
 * Currently, string dimensions are the only bindings which might appear as a {@link String} or a {@link String[]}, so
 * numbers are eliminated from the set of 'unknown' bindings to check as they are encountered.
 */
public class RowBasedExpressionColumnValueSelector extends ExpressionColumnValueSelector
{
  private final List<String> unknownColumns;
  private final Expr.BindingDetails baseExprBindingDetails;
  private final Set<String> ignoredColumns;
  private final Int2ObjectMap<Expr> transformedCache;

  public RowBasedExpressionColumnValueSelector(
      Expr expression,
      Expr.BindingDetails baseExprBindingDetails,
      Expr.ObjectBinding bindings,
      Set<String> unknownColumnsSet
  )
  {
    super(expression, bindings);
    this.unknownColumns = unknownColumnsSet.stream()
                                           .filter(x -> !baseExprBindingDetails.getArrayBindings().contains(x))
                                           .collect(Collectors.toList());
    this.baseExprBindingDetails = baseExprBindingDetails;
    this.ignoredColumns = new HashSet<>();
    this.transformedCache = new Int2ObjectArrayMap<>(unknownColumns.size());
  }

  @Override
  public ExprEval getObject()
  {
    // check to find any arrays for this row
    List<String> arrayBindings = unknownColumns.stream().filter(this::isBindingArray).collect(Collectors.toList());

    // eliminate anything that will never be an array
    if (ignoredColumns.size() > 0) {
      unknownColumns.removeAll(ignoredColumns);
      ignoredColumns.clear();
    }

    // if there are arrays, we need to transform the expression to one that applies each value of the array to the
    // base expression, we keep a cache of transformed expressions to minimize extra work
    if (arrayBindings.size() > 0) {
      final int key = arrayBindings.hashCode();
      if (transformedCache.containsKey(key)) {
        return transformedCache.get(key).eval(bindings);
      }
      Expr transformed = Parser.applyUnappliedBindings(expression, baseExprBindingDetails, arrayBindings);
      transformedCache.put(key, transformed);
      return transformed.eval(bindings);
    }
    // no arrays for this row, evaluate base expression
    return expression.eval(bindings);
  }

  /**
   * Check if row value binding for identifier is an array, adding identifiers that retrieve {@link Number} to a set
   * of 'unknowns' to eliminate by side effect
   */
  private boolean isBindingArray(String x)
  {
    Object binding = bindings.get(x);
    if (binding != null) {
      if (binding instanceof String[]) {
        return true;
      } else if (binding instanceof Number) {
        ignoredColumns.add(x);
      }
    }
    return false;
  }
}
