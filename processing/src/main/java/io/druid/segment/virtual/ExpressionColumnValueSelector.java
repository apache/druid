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

package io.druid.segment.virtual;

import com.google.common.base.Preconditions;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnValueSelector;

import javax.annotation.Nonnull;

public class ExpressionColumnValueSelector implements ColumnValueSelector<ExprEval>
{
  private final Expr.ObjectBinding bindings;
  private final Expr expression;

  public ExpressionColumnValueSelector(Expr expression, Expr.ObjectBinding bindings)
  {
    this.bindings = Preconditions.checkNotNull(bindings, "bindings");
    this.expression = Preconditions.checkNotNull(expression, "expression");
  }

  @Override
  public double getDouble()
  {
    // No Assert for null handling as ExprEval already have it.
    return getObject().asDouble();
  }

  @Override
  public float getFloat()
  {
    // No Assert for null handling as ExprEval already have it.
    return (float) getObject().asDouble();
  }

  @Override
  public long getLong()
  {
    // No Assert for null handling as ExprEval already have it.
    return getObject().asLong();
  }

  @Override
  public Class<ExprEval> classOfObject()
  {
    return ExprEval.class;
  }

  @Nonnull
  @Override
  public ExprEval getObject()
  {
    return expression.eval(bindings);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("expression", expression);
    inspector.visit("bindings", bindings);
  }

  @Override
  public boolean isNull()
  {
    return getObject().isNull();
  }
}
