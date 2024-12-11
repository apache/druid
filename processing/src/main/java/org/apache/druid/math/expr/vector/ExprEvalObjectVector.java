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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;

public final class ExprEvalObjectVector extends ExprEvalVector<Object[]>
{
  @Nullable
  private long[] longs;
  @Nullable
  private double[] doubles;

  @Nullable
  private boolean[] numericNulls;

  private final ExpressionType type;

  public ExprEvalObjectVector(Object[] values, ExpressionType type)
  {
    super(values, null);
    this.type = type;

    if (type.isNumeric()) {
      // Cannot use ExprEvalObjectSelector on types that are innately numbers.
      throw DruidException.defensive("Expression of type[%s] is numeric", type);
    }
  }

  private void computeNumbers()
  {
    if (longs == null) {
      longs = new long[values.length];
      doubles = new double[values.length];
      numericNulls = new boolean[values.length];
      boolean isString = type.is(ExprType.STRING);
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
            numericNulls[i] = NullHandling.sqlCompatible();
          }
        } else {
          // ARRAY, COMPLEX
          final ExprEval<?> valueEval = ExprEval.ofType(type, values[i]).castTo(ExpressionType.DOUBLE);
          longs[i] = valueEval.asLong();
          doubles[i] = valueEval.asDouble();
          numericNulls[i] = valueEval.isNumericNull();
        }
      }
    }
  }

  @Nullable
  @Override
  public boolean[] getNullVector()
  {
    computeNumbers();
    return numericNulls;
  }

  @Override
  public ExpressionType getType()
  {
    return type;
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

  @Override
  public Object[] getObjectVector()
  {
    return values;
  }
}
