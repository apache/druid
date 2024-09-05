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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.RowIdSupplier;

import javax.annotation.Nullable;

/**
 * Wraps a {@link ColumnValueSelector}, calls {@link ColumnValueSelector#getObject()} and provides primitive numeric
 * accessors based on that object value.
 */
public class ObjectToNumberColumnValueSelector implements ColumnValueSelector<Number>
{
  private final ColumnValueSelector<?> selector;
  private final ExpressionType desiredType;

  @Nullable
  private final RowIdSupplier rowIdSupplier;

  @Nullable
  private Number currentValue;
  private long currentRowId = RowIdSupplier.INIT;

  /**
   * Package-private; create using {@link TypeCastSelectors#makeColumnValueSelector} or
   * {@link TypeCastSelectors#wrapColumnValueSelectorIfNeeded}.
   */
  ObjectToNumberColumnValueSelector(
      final ColumnValueSelector<?> selector,
      final ExpressionType desiredType,
      @Nullable final RowIdSupplier rowIdSupplier
  )
  {
    this.selector = selector;
    this.desiredType = desiredType;
    this.rowIdSupplier = rowIdSupplier;

    if (!desiredType.isNumeric()) {
      throw DruidException.defensive("Expected numeric type, got[%s]", desiredType);
    }
  }

  @Override
  public double getDouble()
  {
    final Number n = computeIfNeeded();
    return n == null ? NullHandling.ZERO_DOUBLE : n.doubleValue();
  }

  @Override
  public float getFloat()
  {
    final Number n = computeIfNeeded();
    return n == null ? NullHandling.ZERO_FLOAT : n.floatValue();
  }

  @Override
  public long getLong()
  {
    final Number n = computeIfNeeded();
    return n == null ? NullHandling.ZERO_LONG : n.longValue();
  }

  @Override
  public boolean isNull()
  {
    return computeIfNeeded() == null;
  }

  @Nullable
  @Override
  public Number getObject()
  {
    return computeIfNeeded();
  }

  @Override
  public Class<Number> classOfObject()
  {
    return Number.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("rowIdSupplier", rowIdSupplier);
  }

  @Nullable
  private Number computeIfNeeded()
  {
    if (rowIdSupplier == null) {
      return eval();
    } else {
      final long rowId = rowIdSupplier.getRowId();

      if (currentRowId != rowId) {
        currentValue = eval();
        currentRowId = rowId;
      }

      return currentValue;
    }
  }

  @Nullable
  private Number eval()
  {
    return (Number) TypeCastSelectors.bestEffortCoerce(selector.getObject(), desiredType);
  }
}
