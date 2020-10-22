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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nullable;
import java.util.Arrays;

public final class ExprEvalLongVector extends ExprEvalVector<long[]>
{
  public ExprEvalLongVector(long[] values, @Nullable boolean[] nulls)
  {
    super(values, nulls);
  }

  @Override
  public ExprType getType()
  {
    return ExprType.LONG;
  }

  @Override
  public long[] getLongVector()
  {
    return values;
  }

  @Override
  public double[] getDoubleVector()
  {
    return Arrays.stream(values).asDoubleStream().toArray();
  }

  @Override
  public <E> E asObjectVector(ExprType type)
  {
    switch (type) {
      case STRING:
        String[] s = new String[values.length];
        if (nulls != null) {
          for (int i = 0; i < values.length; i++) {
            s[i] = nulls[i] ? null : String.valueOf(values[i]);
          }
        }
        return (E) s;
      default:
        throw new IAE("Cannot convert %s to %s object vector", getType(), type);
    }
  }
}
