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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nonnull;

/**
 * Like {@link ExpressionColumnValueSelector}, but caches the most recently computed value and re-uses it in the case
 * of runs in the underlying column. This is especially useful for the __time column, where we expect runs.
 */
public class SingleLongInputCachingExpressionColumnValueSelector implements ColumnValueSelector<ExprEval>
{
  enum Validity
  {
    NONE,
    DOUBLE,
    LONG,
    EVAL
  }

  private final ColumnValueSelector selector;
  private final Expr expression;
  private final SingleInputBindings bindings = new SingleInputBindings();

  // Last read input value
  private long lastInput;

  // Last computed output values (validity determined by "validity" field)
  private Validity validity = Validity.NONE;
  private double lastDoubleOutput;
  private long lastLongOutput;
  private ExprEval lastEvalOutput;

  public SingleLongInputCachingExpressionColumnValueSelector(
      final ColumnValueSelector selector,
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
  public double getDouble()
  {
    // No Assert for null handling as delegate selector already have it.
    final long currentInput = selector.getLong();

    if (lastInput == currentInput && validity == Validity.DOUBLE) {
      return lastDoubleOutput;
    } else {
      final double output = eval(currentInput).asDouble();
      lastInput = currentInput;
      lastDoubleOutput = output;
      validity = Validity.DOUBLE;
      return output;
    }
  }

  @Override
  public float getFloat()
  {
    // No Assert for null handling as delegate selector already have it.
    return (float) getDouble();
  }

  @Override
  public long getLong()
  {
    // No Assert for null handling as delegate selector already have it.
    final long currentInput = selector.getLong();

    if (lastInput == currentInput && validity == Validity.LONG) {
      return lastLongOutput;
    } else {
      final long output = eval(currentInput).asLong();
      lastInput = currentInput;
      lastLongOutput = output;
      validity = Validity.LONG;
      return output;
    }
  }

  @Nonnull
  @Override
  public ExprEval getObject()
  {
    final long currentInput = selector.getLong();

    if (lastInput == currentInput && validity == Validity.EVAL) {
      return lastEvalOutput;
    } else {
      final ExprEval output = eval(currentInput);
      lastInput = currentInput;
      lastEvalOutput = output;
      validity = Validity.EVAL;
      return output;
    }
  }

  @Override
  public Class<ExprEval> classOfObject()
  {
    return ExprEval.class;
  }

  private ExprEval eval(final long value)
  {
    bindings.set(value);
    return expression.eval(bindings);
  }

  @Override
  public boolean isNull()
  {
    // It is possible for an expression to have a non-null String value but it can return null when parsed
    // as a primitive long/float/double.
    // ExprEval.isNumericNull checks whether the parsed primitive value is null or not.
    return getObject().isNumericNull();
  }
}
