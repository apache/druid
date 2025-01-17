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

/**
 * {@link ExprEvalVector} backed directly by an underlying {@link Expr.VectorInputBinding}
 */
public class ExprEvalBindingVector<T> implements ExprEvalVector<T>
{
  private final ExpressionType expressionType;
  private final Expr.VectorInputBinding bindings;
  private final String bindingName;

  @Nullable
  private long[] longs;
  @Nullable
  private double[] doubles;

  @Nullable
  private boolean[] numericNulls;

  public ExprEvalBindingVector(
      ExpressionType expressionType,
      Expr.VectorInputBinding bindings,
      String name
  )
  {
    this.expressionType = expressionType;
    this.bindings = bindings;
    this.bindingName = name;
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
    return (T) bindings.getObjectVector(bindingName);
  }

  @Override
  public long[] getLongVector()
  {
    if (expressionType.isNumeric()) {
      return bindings.getLongVector(bindingName);
    }
    computeNumbers();
    return longs;
  }

  @Override
  public double[] getDoubleVector()
  {
    if (expressionType.isNumeric()) {
      return bindings.getDoubleVector(bindingName);
    }
    computeNumbers();
    return doubles;
  }

  @Nullable
  @Override
  public boolean[] getNullVector()
  {
    if (expressionType.isNumeric()) {
      return bindings.getNullVector(bindingName);
    }
    computeNumbers();
    return numericNulls;
  }

  @Override
  public Object[] getObjectVector()
  {
    if (expressionType.is(ExprType.LONG)) {
      final long[] values = bindings.getLongVector(bindingName);
      final boolean[] nulls = bindings.getNullVector(bindingName);
      final Long[] objects = new Long[values.length];
      if (nulls != null) {
        for (int i = 0; i < values.length; i++) {
          objects[i] = nulls[i] ? null : values[i];
        }
      } else {
        for (int i = 0; i < values.length; i++) {
          objects[i] = values[i];
        }
      }
      return objects;
    } else if (expressionType.is(ExprType.DOUBLE)) {
      final double[] values = bindings.getDoubleVector(bindingName);
      final boolean[] nulls = bindings.getNullVector(bindingName);
      Double[] objects = new Double[values.length];
      if (nulls != null) {
        for (int i = 0; i < values.length; i++) {
          objects[i] = nulls[i] ? null : values[i];
        }
      } else {
        for (int i = 0; i < values.length; i++) {
          objects[i] = values[i];
        }
      }
      return objects;
    }
    return bindings.getObjectVector(bindingName);
  }

  private void computeNumbers()
  {
    final Object[] values = getObjectVector();
    if (longs == null) {
      longs = new long[values.length];
      doubles = new double[values.length];
      numericNulls = new boolean[values.length];
      boolean isString = expressionType.is(ExprType.STRING);
      for (int i = 0; i < values.length; i++) {
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
}
