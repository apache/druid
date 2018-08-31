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

package org.apache.druid.segment;

import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

public class ConstantExprEvalSelector implements ColumnValueSelector<ExprEval>
{
  private final long longValue;
  private final float floatValue;
  private final double doubleValue;
  private final ExprEval eval;
  private final boolean isNull;

  public ConstantExprEvalSelector(final ExprEval eval)
  {
    this.eval = Preconditions.checkNotNull(eval, "eval");

    if (NullHandling.sqlCompatible() && eval.isNumericNull()) {
      longValue = 0L;
      floatValue = 0f;
      doubleValue = 0d;
      isNull = true;
    } else {
      longValue = eval.asLong();
      doubleValue = eval.asDouble();
      floatValue = (float) doubleValue;
      isNull = false;
    }
  }

  @Override
  public double getDouble()
  {
    return doubleValue;
  }

  @Override
  public float getFloat()
  {
    return floatValue;
  }

  @Override
  public long getLong()
  {
    return longValue;
  }

  @Override
  public ExprEval getObject()
  {
    return eval;
  }

  @Override
  public Class<ExprEval> classOfObject()
  {
    return ExprEval.class;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    // Nothing here: eval's class can vary, but getObject is not @CalledFromHotLoop
  }

  @Override
  public boolean isNull()
  {
    return isNull;
  }
}
