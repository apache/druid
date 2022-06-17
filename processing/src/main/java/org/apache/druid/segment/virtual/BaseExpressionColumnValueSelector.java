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

package org.apache.druid.segment.virtual;

import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.RowIdSupplier;

import javax.annotation.Nullable;

/**
 * Base class for many (although not all) {@code ColumnValueSelector<ExprEval>}.
 */
public abstract class BaseExpressionColumnValueSelector implements ColumnValueSelector<ExprEval>
{
  @Nullable
  private final RowIdSupplier rowIdSupplier;
  private long currentRowId = RowIdSupplier.INIT;
  private ExprEval<?> currentEval;

  protected BaseExpressionColumnValueSelector(@Nullable RowIdSupplier rowIdSupplier)
  {
    this.rowIdSupplier = rowIdSupplier;
  }

  @Override
  public double getDouble()
  {
    // No assert for null handling as ExprEval already has it.
    return computeCurrentEval().asDouble();
  }

  @Override
  public float getFloat()
  {
    // No assert for null handling as ExprEval already has it.
    return (float) computeCurrentEval().asDouble();
  }

  @Override
  public long getLong()
  {
    // No assert for null handling as ExprEval already has it.
    return computeCurrentEval().asLong();
  }

  @Override
  public boolean isNull()
  {
    // It is possible for an expression to have a non-null String value, but be null when treated as long/float/double.
    // Check specifically for numeric nulls, which matches the expected behavior of ColumnValueSelector.isNull.
    return computeCurrentEval().isNumericNull();
  }

  @Nullable
  @Override
  public ExprEval<?> getObject()
  {
    return computeCurrentEval();
  }

  @Override
  public Class<ExprEval> classOfObject()
  {
    return ExprEval.class;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("rowIdSupplier", rowIdSupplier);
  }

  /**
   * Implementations override this.
   */
  protected abstract ExprEval<?> eval();

  /**
   * Call {@link #eval()} or use {@code currentEval} as appropriate.
   */
  private ExprEval<?> computeCurrentEval()
  {
    if (rowIdSupplier == null) {
      return eval();
    } else {
      final long rowId = rowIdSupplier.getRowId();

      if (currentRowId != rowId) {
        currentEval = eval();
        currentRowId = rowId;
      }

      return currentEval;
    }
  }
}
