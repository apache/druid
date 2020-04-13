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
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Like {@link ExpressionColumnValueSelector}, but caches the most recently computed value and re-uses it in the case
 * of runs in the underlying column. This is especially useful for the __time column, where we expect runs.
 */
public class SingleLongInputCachingExpressionColumnValueSelector implements ColumnValueSelector<ExprEval>
{
  private static final int CACHE_SIZE = 1000;

  private final ColumnValueSelector selector;
  private final Expr expression;
  private final SingleInputBindings bindings = new SingleInputBindings();

  @Nullable
  private final LruEvalCache lruEvalCache;

  // Last read input value.
  private long lastInput;

  // Last computed output value, or null if there is none.
  @Nullable
  private ExprEval lastOutput;

  public SingleLongInputCachingExpressionColumnValueSelector(
      final ColumnValueSelector selector,
      final Expr expression,
      final boolean useLruCache
  )
  {
    // Verify expression has just one binding.
    if (expression.analyzeInputs().getRequiredBindings().size() != 1) {
      throw new ISE("WTF?! Expected expression with just one binding");
    }

    this.selector = Preconditions.checkNotNull(selector, "selector");
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.lruEvalCache = useLruCache ? new LruEvalCache() : null;
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
    return getObject().asDouble();
  }

  @Override
  public float getFloat()
  {
    return (float) getObject().asDouble();
  }

  @Override
  public long getLong()
  {
    return getObject().asLong();
  }

  @Nonnull
  @Override
  public ExprEval getObject()
  {
    // things can still call this even when underlying selector is null (e.g. ExpressionColumnValueSelector#isNull)
    if (selector.isNull()) {
      return ExprEval.ofLong(null);
    }
    // No assert for null handling, as the delegate selector already has it.
    final long input = selector.getLong();
    final boolean cached = input == lastInput && lastOutput != null;

    if (!cached) {
      if (lruEvalCache == null) {
        bindings.set(input);
        lastOutput = expression.eval(bindings);
      } else {
        lastOutput = lruEvalCache.compute(input);
      }

      lastInput = input;
    }

    return lastOutput;
  }

  @Override
  public Class<ExprEval> classOfObject()
  {
    return ExprEval.class;
  }

  @Override
  public boolean isNull()
  {
    // It is possible for an expression to have a non-null String value but it can return null when parsed
    // as a primitive long/float/double.
    // ExprEval.isNumericNull checks whether the parsed primitive value is null or not.
    return getObject().isNumericNull();
  }

  public class LruEvalCache
  {
    private final Long2ObjectLinkedOpenHashMap<ExprEval> m = new Long2ObjectLinkedOpenHashMap<>();

    public ExprEval compute(final long n)
    {
      ExprEval value = m.getAndMoveToFirst(n);

      if (value == null) {
        bindings.set(n);
        value = expression.eval(bindings);
        m.putAndMoveToFirst(n, value);

        if (m.size() > CACHE_SIZE) {
          m.removeLast();
        }
      }

      return value;
    }
  }
}
