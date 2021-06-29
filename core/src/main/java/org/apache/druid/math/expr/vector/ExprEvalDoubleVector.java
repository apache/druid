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

import org.apache.druid.math.expr.ExprType;

import java.util.Arrays;

public final class ExprEvalDoubleVector extends ExprEvalVector<double[]>
{
  public ExprEvalDoubleVector(double[] values, boolean[] nulls)
  {
    super(values, nulls);
  }

  @Override
  public ExprType getType()
  {
    return ExprType.DOUBLE;
  }

  @Override
  public double[] values()
  {
    return values;
  }

  @Override
  public long[] getLongVector()
  {
    return Arrays.stream(values).mapToLong(d -> (long) d).toArray();
  }

  @Override
  public double[] getDoubleVector()
  {
    return values;
  }

  @Override
  public Object[] getObjectVector()
  {
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
}
