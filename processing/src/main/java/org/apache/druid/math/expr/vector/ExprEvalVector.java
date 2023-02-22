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
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;
import java.lang.reflect.Array;

/**
 * Result of {@link ExprVectorProcessor#evalVector} which wraps the actual evaluated results of the operation over the
 * input vector(s). Methods to get actual results mirror vectorized value and object selectors.
 *
 * The generic parameter T should be the native java array type of the vector result (long[], Object[], etc.)
 */
public abstract class ExprEvalVector<T>
{
  final T values;
  @Nullable
  final boolean[] nulls;

  public ExprEvalVector(T values, @Nullable boolean[] nulls)
  {
    this.values = values;
    this.nulls = nulls;
  }

  public T values()
  {
    return values;
  }

  @Nullable
  public Object get(int index)
  {
    if (nulls == null || NullHandling.replaceWithDefault() || !nulls[index]) {
      return Array.get(values, index);
    }
    return null;
  }

  @Nullable
  public boolean[] getNullVector()
  {
    return nulls;
  }

  public abstract ExpressionType getType();

  public abstract long[] getLongVector();

  public abstract double[] getDoubleVector();

  public abstract Object[] getObjectVector();
}
