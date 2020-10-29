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
import org.apache.druid.math.expr.Exprs;

public abstract class CastToTypeVectorProcessor<TOutput> implements ExprVectorProcessor<TOutput>
{
  protected final ExprVectorProcessor<?> delegate;

  protected CastToTypeVectorProcessor(ExprVectorProcessor<?> delegate)
  {
    this.delegate = delegate;
  }

  public static <T> ExprVectorProcessor<T> cast(ExprVectorProcessor<?> delegate, ExprType type)
  {
    final ExprVectorProcessor<?> caster;
    if (delegate.getOutputType() == type) {
      caster = delegate;
    } else {
      switch (type) {
        case STRING:
          caster = new CastToStringVectorProcessor(delegate);
          break;
        case LONG:
          caster = new CastToLongVectorProcessor(delegate);
          break;
        case DOUBLE:
          caster = new CastToDoubleVectorProcessor(delegate);
          break;
        default:
          throw Exprs.cannotVectorize();
      }
    }
    return (ExprVectorProcessor<T>) caster;
  }
}
