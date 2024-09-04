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

package org.apache.druid.frame.write.cast;

import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

/**
 * Wraps a {@link ColumnValueSelector}, calls {@link ColumnValueSelector#getObject()}, and casts that value using
 * {@link ExprEval}. The object is interpreted using {@link ExprEval#ofType}.
 */
public class ObjectToExprEvalColumnValueSelector implements ColumnValueSelector<Object>
{
  private final ColumnValueSelector<?> selector;
  @Nullable
  private final ExpressionType selectorType;
  private final ExpressionType desiredType;
  @Nullable
  private final RowIdSupplier rowIdSupplier;

  public ObjectToExprEvalColumnValueSelector(
      final ColumnValueSelector<?> selector,
      @Nullable final ColumnType selectorType,
      final ColumnType desiredType,
      @Nullable final RowIdSupplier rowIdSupplier
  )
  {
    this.selector = selector;
    this.selectorType = ExpressionType.fromColumnType(selectorType);
    this.desiredType = ExpressionType.fromColumnType(desiredType);
    this.rowIdSupplier = rowIdSupplier;
  }

  @Override
  public double getDouble()
  {
    return eval().asDouble();
  }

  @Override
  public float getFloat()
  {
    return (float) eval().asDouble();
  }

  @Override
  public long getLong()
  {
    return eval().asLong();
  }

  @Override
  public boolean isNull()
  {
    return eval().isNumericNull();
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return eval().value();
  }

  @Override
  public Class<Object> classOfObject()
  {
    return Object.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("rowIdSupplier", rowIdSupplier);
  }

  private ExprEval<?> eval()
  {
    final Object obj = selector.getObject();
    if (obj == null) {
      return ExprEval.of(null);
    } else {
      return ExprEval.ofType(selectorType, obj).castTo(desiredType);
    }
  }
}
