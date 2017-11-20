/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.virtual;

import com.google.common.base.Preconditions;
import io.druid.java.util.common.ISE;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.BaseLongColumnValueSelector;

public class SingleLongInputCachingExpressionDimensionSelector extends BaseSingleValueDimensionSelector
{
  private final BaseLongColumnValueSelector selector;
  private final Expr expression;
  private final SingleInputBindings bindings = new SingleInputBindings();

  // Is this the first row?
  private boolean first = true;

  // Last read input value
  private long lastInput;

  // Last computed output value
  private String lastOutput;

  public SingleLongInputCachingExpressionDimensionSelector(
      final BaseLongColumnValueSelector selector,
      final Expr expression
  )
  {
    // Verify expression has just one binding.
    if (Parser.findRequiredBindings(expression).size() != 1) {
      throw new ISE("WTF?! Expected expression with just one binding");
    }

    this.selector = Preconditions.checkNotNull(selector, "selector");
    this.expression = Preconditions.checkNotNull(expression, "expression");
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("expression", expression);
  }

  @Override
  public String getValue()
  {
    final long currentInput = selector.getLong();

    if (first || lastInput != currentInput) {
      final String output = eval(currentInput);
      lastInput = currentInput;
      lastOutput = output;
      first = false;
    }

    return lastOutput;
  }

  private String eval(final long value)
  {
    bindings.set(value);
    return expression.eval(bindings).asString();
  }
}
