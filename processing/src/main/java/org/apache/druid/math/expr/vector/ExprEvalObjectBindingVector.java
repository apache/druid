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

package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * {@link ExprEvalVector} backed directly by an underlying {@link Expr.VectorInputBinding} for object type bindings
 */
public class ExprEvalObjectBindingVector<T> implements ExprEvalVector<T>
{
  private final ExpressionType expressionType;
  private final Expr.VectorInputBinding bindings;
  private final String bindingName;
  private final Supplier<VectorCache> vectorCacheSupplier;

  @Nullable
  private long[] longs;
  @Nullable
  private double[] doubles;

  @Nullable
  private boolean[] numericNulls;

  public ExprEvalObjectBindingVector(
      ExpressionType expressionType,
      Expr.VectorInputBinding bindings,
      String name,
      Supplier<VectorCache> vectorCacheSupplier
  )
  {
    this.expressionType = expressionType;
    this.bindings = bindings;
    this.bindingName = name;
    this.vectorCacheSupplier = vectorCacheSupplier;
  }

  @Override
  public ExpressionType getType()
  {
    return expressionType;
  }

  @Override
  public T values()
  {
    return (T) getObjectVector();
  }

  @Override
  public long[] getLongVector()
  {
    computeNumbers();
    return longs;
  }

  @Override
  public double[] getDoubleVector()
  {
    computeNumbers();
    return doubles;
  }

  @Nullable
  @Override
  public boolean[] getNullVector()
  {
    computeNumbers();
    return numericNulls;
  }

  @Override
  public Object[] getObjectVector()
  {
    return bindings.getObjectVector(bindingName);
  }

  @Override
  public boolean elementAsBoolean(int index)
  {
    final Object[] objects = bindings.getObjectVector(bindingName);
    if (expressionType.is(ExprType.STRING)) {
      return Evals.asBoolean((String) objects[index]);
    } else {
      return ExprEval.ofType(expressionType, objects[index]).asBoolean();
    }
  }

  private void computeNumbers()
  {
    final Object[] values = bindings.getObjectVector(bindingName);
    if (longs == null) {
      longs = vectorCacheSupplier.get().longs;
      doubles = vectorCacheSupplier.get().doubles;
      numericNulls = vectorCacheSupplier.get().nulls;
      boolean isString = expressionType.is(ExprType.STRING);
      for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
        if (isString) {
          Number n = ExprEval.computeNumber(Evals.asString(values[i]));
          if (n != null) {
            longs[i] = n.longValue();
            doubles[i] = n.doubleValue();
            numericNulls[i] = false;
          } else {
            longs[i] = 0L;
            doubles[i] = 0.0;
            numericNulls[i] = true;
          }
        } else {
          // ARRAY, COMPLEX
          final ExprEval<?> valueEval = ExprEval.ofType(expressionType, values[i]);
          longs[i] = valueEval.asLong();
          doubles[i] = valueEval.asDouble();
          numericNulls[i] = valueEval.isNumericNull();
        }
      }
    }
  }

  public static VectorCache createCache(int maxSize)
  {
    return new VectorCache(maxSize);
  }

  /**
   * Container to re-use a allocated objects across various {@link ExprEvalObjectBindingVector} instances
   */
  public static class VectorCache
  {
    private final long[] longs;
    private final double[] doubles;
    private final boolean[] nulls;

    private VectorCache(int maxSize)
    {
      this.longs = new long[maxSize];
      this.doubles = new double[maxSize];
      this.nulls = new boolean[maxSize];
    }
  }
}
