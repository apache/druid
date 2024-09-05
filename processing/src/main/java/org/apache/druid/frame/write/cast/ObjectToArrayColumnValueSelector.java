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

import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.RowIdSupplier;

import javax.annotation.Nullable;

/**
 * Wraps a {@link ColumnValueSelector}, calls {@link ColumnValueSelector#getObject()}, interprets that value using
 * {@link ExprEval#ofType}, and casts it using {@link ExprEval#castTo}.
 */
public class ObjectToArrayColumnValueSelector implements ColumnValueSelector<Object[]>
{
  private final ColumnValueSelector<?> selector;
  @Nullable
  private final ExpressionType desiredType;
  @Nullable
  private final RowIdSupplier rowIdSupplier;

  public ObjectToArrayColumnValueSelector(
      final ColumnValueSelector<?> selector,
      final ExpressionType desiredType,
      @Nullable final RowIdSupplier rowIdSupplier
  )
  {
    this.selector = selector;
    this.desiredType = desiredType;
    this.rowIdSupplier = rowIdSupplier;

    if (!desiredType.isArray() || desiredType.getElementType() == null) {
      throw DruidException.defensive("Expected array with nonnull element type, got[%s]", desiredType);
    }
  }

  @Override
  public double getDouble()
  {
    throw DruidException.defensive("Unexpected call to getDouble on array selector");
  }

  @Override
  public float getFloat()
  {
    throw DruidException.defensive("Unexpected call to getFloat on array selector");
  }

  @Override
  public long getLong()
  {
    throw DruidException.defensive("Unexpected call to getLong on array selector");
  }

  @Override
  public boolean isNull()
  {
    throw DruidException.defensive("Unexpected call to isNull on array selector");
  }

  @Nullable
  @Override
  public Object[] getObject()
  {
    return (Object[]) TypeCastSelectors.bestEffortCoerce(selector.getObject(), desiredType);
  }

  @Override
  public Class<Object[]> classOfObject()
  {
    return Object[].class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("rowIdSupplier", rowIdSupplier);
  }
}
