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

import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * {@link ExprEvalVector} backed directly by an underlying {@link Expr.VectorInputBinding} for numeric type bindings
 */
public class ExprEvalNumericBindingVector<T> implements ExprEvalVector<T>
{
  private final ExpressionType expressionType;
  private final Expr.VectorInputBinding bindings;
  private final String bindingName;
  private final Supplier<VectorCache> vectorCacheSupplier;

  @MonotonicNonNull
  private Object[] objects;

  public ExprEvalNumericBindingVector(
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
    if (expressionType.is(ExprType.LONG)) {
      return (T) getLongVector();
    } else if (expressionType.is(ExprType.DOUBLE)) {
      return (T) getDoubleVector();
    }
    throw DruidException.defensive(
        "Non-numeric type[%s] for binding[%s] used with numeric binding vector",
        expressionType,
        bindingName
    );
  }

  @Override
  public long[] getLongVector()
  {
    return bindings.getLongVector(bindingName);
  }

  @Override
  public double[] getDoubleVector()
  {
    return bindings.getDoubleVector(bindingName);
  }

  @Nullable
  @Override
  public boolean[] getNullVector()
  {
    return bindings.getNullVector(bindingName);
  }

  @Override
  public Object[] getObjectVector()
  {
    if (objects == null) {
      objects = vectorCacheSupplier.get().objects;
      if (expressionType.is(ExprType.LONG)) {
        final long[] longs = bindings.getLongVector(bindingName);
        final boolean[] numericNulls = bindings.getNullVector(bindingName);
        if (numericNulls != null) {
          for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
            objects[i] = numericNulls[i] ? null : longs[i];
          }
        } else {
          for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
            objects[i] = longs[i];
          }
        }
      } else if (expressionType.is(ExprType.DOUBLE)) {
        objects = vectorCacheSupplier.get().objects;
        final double[] doubles = bindings.getDoubleVector(bindingName);
        final boolean[] numericNulls = bindings.getNullVector(bindingName);
        if (numericNulls != null) {
          for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
            objects[i] = numericNulls[i] ? null : doubles[i];
          }
        } else {
          for (int i = 0; i < bindings.getCurrentVectorSize(); i++) {
            objects[i] = doubles[i];
          }
        }
      } else {
        throw DruidException.defensive(
            "Non-numeric type[%s] for binding[%s] used with numeric binding vector",
            expressionType,
            bindingName
        );
      }
    }
    return objects;
  }

  @Override
  public boolean elementAsBoolean(int index)
  {
    if (expressionType.is(ExprType.LONG)) {
      final long[] longs = bindings.getLongVector(bindingName);
      final boolean[] numericNulls = bindings.getNullVector(bindingName);
      if (numericNulls != null && numericNulls[index]) {
        return Evals.asBoolean(0L);
      }
      return Evals.asBoolean(longs[index]);
    } else if (expressionType.is(ExprType.DOUBLE)) {
      final double[] doubles = bindings.getDoubleVector(bindingName);
      final boolean[] numericNulls = bindings.getNullVector(bindingName);
      if (numericNulls != null && numericNulls[index]) {
        return Evals.asBoolean(0.0);
      }
      return Evals.asBoolean(doubles[index]);
    }
    throw DruidException.defensive(
        "Non-numeric type[%s] for binding[%s] used with numeric binding vector",
        expressionType,
        bindingName
    );
  }

  public static VectorCache createCache(int maxSize)
  {
    return new VectorCache(maxSize);
  }

  /**
   * Container to re-use a allocated objects across various {@link ExprEvalNumericBindingVector} instances
   */
  public static class VectorCache
  {
    private final Object[] objects;

    private VectorCache(int maxSize)
    {
      this.objects = new Object[maxSize];
    }
  }
}
