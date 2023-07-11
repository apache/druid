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
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.ExprEval;
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

  public ExprEvalObjectVector(Object[] values)
  {
    super(values, null);
  }

  private void computeNumbers()
  {
    if (longs == null) {
      longs = new long[values.length];
      doubles = new double[values.length];
      numericNulls = new boolean[values.length];
      for (int i = 0; i < values.length; i++) {
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
    return ExpressionType.STRING;
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
