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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OpportunisticMultiValueStringExpressionColumnValueSelector extends ExpressionColumnValueSelector
{
  private final List<String> unknownColumns;
  private final Expr.BindingDetails baseExprBindingDetails;
  private final Set<String> ignoredColumns;
  private final Int2ObjectMap<Expr> transformedCache;

  public OpportunisticMultiValueStringExpressionColumnValueSelector(
      Expr expression,
      Expr.BindingDetails baseExprBindingDetails,
      Expr.ObjectBinding bindings,
      Set<String> unknownColumnsSet
  )
  {
    super(expression, bindings);
    this.unknownColumns = new ArrayList<>(unknownColumnsSet);
    this.baseExprBindingDetails = baseExprBindingDetails;
    this.ignoredColumns = new HashSet<>();
    this.transformedCache = new Int2ObjectArrayMap(unknownColumns.size());
  }

  @Override
  public ExprEval getObject()
  {
    List<String> arrayBindings =
        unknownColumns.stream()
                      .filter(x -> !baseExprBindingDetails.getArrayVariables().contains(x) && isBindingArray(x))
                      .collect(Collectors.toList());

    if (ignoredColumns.size() > 0) {
      unknownColumns.removeAll(ignoredColumns);
      ignoredColumns.clear();
    }

    if (arrayBindings.size() > 0) {
      final int key = arrayBindings.hashCode();
      if (transformedCache.containsKey(key)) {
        return transformedCache.get(key).eval(bindings);
      }
      Expr transformed = Parser.applyUnappliedIdentifiers(expression, baseExprBindingDetails, arrayBindings);
      transformedCache.put(key, transformed);
      return transformed.eval(bindings);
    }
    return expression.eval(bindings);
  }

  private boolean isBindingArray(String x)
  {
    Object binding = bindings.get(x);
    if (binding != null) {
      if (binding instanceof String[] && ((String[]) binding).length > 1) {
        return true;
      } else if (binding instanceof Number) {
        ignoredColumns.add(x);
      }
    }
    return false;
  }
}
