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
import com.google.common.base.Supplier;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

/**
 * Like {@link ExpressionColumnValueSelector}, but caches results for the first CACHE_SIZE dictionary IDs of
 * a string column. Must only be used on selectors with dictionaries.
 */
public class SingleStringInputCachingExpressionColumnValueSelector implements ColumnValueSelector<ExprEval>
{
  private static final int CACHE_SIZE = 1000;

  private final DimensionSelector selector;
  private final Expr expression;
  private final Expr.ObjectBinding bindings;
  @Nullable
  private final ExprEval[] arrayEvalCache;
  @Nullable
  private final LruEvalCache lruEvalCache;

  public SingleStringInputCachingExpressionColumnValueSelector(
      final DimensionSelector selector,
      final Expr expression
  )
  {
    // Verify expression has just one binding.
    if (expression.analyzeInputs().getRequiredBindings().size() != 1) {
      throw new ISE("WTF?! Expected expression with just one binding");
    }

    this.selector = Preconditions.checkNotNull(selector, "selector");
    this.expression = Preconditions.checkNotNull(expression, "expression");

    final Supplier<Object> inputSupplier = ExpressionSelectors.supplierFromDimensionSelector(selector, false);
    this.bindings = name -> inputSupplier.get();

    if (selector.getValueCardinality() == DimensionDictionarySelector.CARDINALITY_UNKNOWN) {
      throw new ISE("Selector must have a dictionary");
    } else if (selector.getValueCardinality() <= CACHE_SIZE) {
      arrayEvalCache = new ExprEval[selector.getValueCardinality()];
      lruEvalCache = null;
    } else {
      arrayEvalCache = null;
      lruEvalCache = new LruEvalCache(expression, bindings);
    }
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
    // No Assert for null handling as ExprEval already have it.
    return eval().asDouble();
  }

  @Override
  public float getFloat()
  {
    // No Assert for null handling as ExprEval already have it.
    return (float) eval().asDouble();
  }

  @Override
  public long getLong()
  {
    // No Assert for null handling as ExprEval already have it.
    return eval().asLong();
  }

  @Nullable
  @Override
  public ExprEval getObject()
  {
    return eval();
  }

  @Override
  public Class<ExprEval> classOfObject()
  {
    return ExprEval.class;
  }

  private ExprEval eval()
  {
    final IndexedInts row = selector.getRow();

    if (row.size() == 1) {
      final int id = row.get(0);

      if (arrayEvalCache != null) {
        if (arrayEvalCache[id] == null) {
          arrayEvalCache[id] = expression.eval(bindings);
        }
        return arrayEvalCache[id];
      } else {
        assert lruEvalCache != null;
        return lruEvalCache.compute(id);
      }
    }

    return expression.eval(bindings);
  }

  @Override
  public boolean isNull()
  {
    // It is possible for an expression to have a non-null String value but it can return null when parsed
    // as a primitive long/float/double.
    // ExprEval.isNumericNull checks whether the parsed primitive value is null or not.
    return eval().isNumericNull();
  }

  public static class LruEvalCache
  {
    private final Expr expression;
    private final Expr.ObjectBinding bindings;
    private final Int2ObjectLinkedOpenHashMap<ExprEval> m = new Int2ObjectLinkedOpenHashMap<>(CACHE_SIZE);

    public LruEvalCache(final Expr expression, final Expr.ObjectBinding bindings)
    {
      this.expression = expression;
      this.bindings = bindings;
    }

    public ExprEval compute(final int id)
    {
      ExprEval value = m.getAndMoveToFirst(id);

      if (value == null) {
        value = expression.eval(bindings);
        m.putAndMoveToFirst(id, value);

        if (m.size() > CACHE_SIZE) {
          m.removeLast();
        }
      }

      return value;
    }
  }
}
