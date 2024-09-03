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
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.RowIdSupplier;

import javax.annotation.Nullable;

public class ObjectToDoubleColumnValueSelector implements ColumnValueSelector<Double>
{
  private final ColumnValueSelector<?> objectSelector;

  @Nullable
  private final RowIdSupplier rowIdSupplier;

  @Nullable
  private Double currentValue;
  private long currentRowId = RowIdSupplier.INIT;

  public ObjectToDoubleColumnValueSelector(
      final ColumnValueSelector<?> objectSelector,
      @Nullable final RowIdSupplier rowIdSupplier
  )
  {
    this.objectSelector = objectSelector;
    this.rowIdSupplier = rowIdSupplier;
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
  public Double getObject()
  {
    return computeIfNeeded();
  }

  @Override
  public Class<Double> classOfObject()
  {
    return Double.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("objectSelector", objectSelector);
    inspector.visit("rowIdSupplier", rowIdSupplier);
  }

  @Nullable
  private Double computeIfNeeded()
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
  private Double eval()
  {
    final Object obj = objectSelector.getObject();
    if (obj == null) {
      return null;
    } else if (obj instanceof Number) {
      return ((Number) obj).doubleValue();
    } else {
      final ExprEval<?> eval = ExprEval.bestEffortOf(obj);
      return eval.isNumericNull() ? null : eval.asDouble();
    }
  }
}
