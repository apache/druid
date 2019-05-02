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

import net.thisptr.jackson.jq.internal.misc.Strings;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OpportunisticMultiValueStringExpressionColumnValueSelector extends ExpressionColumnValueSelector
{
  private final List<String> unknownColumns;
  private final Set<String> arrayInputs;
  private final Map<String, Expr> transformedCache;

  public OpportunisticMultiValueStringExpressionColumnValueSelector(
      Expr expression,
      Expr.ObjectBinding bindings,
      Set<String> unknownColumnsSet
  )
  {
    super(expression, bindings);
    this.unknownColumns = new ArrayList<>(unknownColumnsSet);
    this.arrayInputs = Parser.findArrayFnBindings(expression);
    this.transformedCache = new HashMap<>();
  }

  @Override
  public ExprEval getObject()
  {
    List<String> arrayBindings =
        unknownColumns.stream().filter(x -> !arrayInputs.contains(x) && isBindingArray(x)).collect(Collectors.toList());

    if (arrayBindings.size() > 0) {
      final String key = Strings.join(",", arrayBindings);
      if (transformedCache.containsKey(key)) {
        return transformedCache.get(key).eval(bindings);
      }
      Expr transformed = Parser.applyUnappliedIdentifiers(expression, arrayBindings);
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
        //      if (binding instanceof String[]) {
        return true;
      }
    }
    return false;
  }
}
