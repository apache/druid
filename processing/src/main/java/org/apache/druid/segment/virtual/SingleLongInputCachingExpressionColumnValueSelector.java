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
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Like {@link ExpressionColumnValueSelector}, but caches the most recently computed value and re-uses it in the case
 * of runs in the underlying column. This is especially useful for the __time column, where we expect runs.
 */
public class SingleLongInputCachingExpressionColumnValueSelector extends BaseExpressionColumnValueSelector
{
  private static final int CACHE_SIZE = 1000;

  private final ColumnValueSelector selector;
  private final Expr expression;
  private final SingleInputBindings bindings = new SingleInputBindings(ExpressionType.LONG);

  @Nullable
  private final LruEvalCache lruEvalCache;

  // Last read input value.
  private long lastInput;

  // Last computed output value, or null if there is none.
  @Nullable
  private ExprEval lastOutput;

  // Computed output value for null.
  @MonotonicNonNull
  private ExprEval nullOutput;

  public SingleLongInputCachingExpressionColumnValueSelector(
      final ColumnValueSelector selector,
      final Expr expression,
      final boolean useLruCache,
      @Nullable RowIdSupplier rowIdSupplier
  )
  {
    super(rowIdSupplier);

    // Verify expression has just one binding.
    if (expression.analyzeInputs().getRequiredBindings().size() != 1) {
      throw new ISE("Expected expression with just one binding");
    }

    this.selector = Preconditions.checkNotNull(selector, "selector");
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.lruEvalCache = useLruCache ? new LruEvalCache() : null;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    super.inspectRuntimeShape(inspector);
    inspector.visit("selector", selector);
    inspector.visit("expression", expression);
  }

  @Nonnull
  @Override
  protected ExprEval<?> eval()
  {
    // things can still call this even when underlying selector is null (e.g. ExpressionColumnValueSelector#isNull)
    if (selector.isNull()) {
      if (nullOutput == null) {
        bindings.set(null);
        nullOutput = expression.eval(bindings);
      }
      return nullOutput;
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
