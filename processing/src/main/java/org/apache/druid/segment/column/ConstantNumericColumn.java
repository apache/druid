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

package org.apache.druid.segment.column;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ConstantVectorSelectors;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

/**
 * A numeric {@link NumericColumn} of a fixed length whose value is the same for every row. Vector selectors are backed
 * by the shared {@link ConstantVectorSelectors} helpers; the scalar selector returns the boxed value directly. Used to
 * fabricate a per-group clustering column for a clustered base table (constant within the group), but is not otherwise
 * specific to clustered segments.
 */
public final class ConstantNumericColumn implements NumericColumn
{
  private final ColumnType type;
  @Nullable
  private final Number value;
  private final int numRows;

  public ConstantNumericColumn(ColumnType type, @Nullable Number value, int numRows)
  {
    this.type = type;
    this.value = value;
    this.numRows = numRows;
  }

  @Override
  public int length()
  {
    return numRows;
  }

  @Override
  public long getLongSingleValueRow(int rowNum)
  {
    return value == null ? 0L : value.longValue();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return new ConstantNumericValueSelector(value);
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
  {
    return ConstantVectorSelectors.vectorValueSelector(offset, value);
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    return ConstantVectorSelectors.vectorObjectSelector(offset, value);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("type", type.asTypeString());
    inspector.visit("value", String.valueOf(value));
  }

  @Override
  public void close()
  {
    // nothing to close
  }

  /**
   * Constant numeric {@link ColumnValueSelector}. {@link #getObject()} returns the boxed value (or null)
   */
  private static final class ConstantNumericValueSelector implements ColumnValueSelector<Object>
  {
    @Nullable
    private final Number value;

    private ConstantNumericValueSelector(@Nullable Number value)
    {
      this.value = value;
    }

    @Override
    public double getDouble()
    {
      return value == null ? 0d : value.doubleValue();
    }

    @Override
    public float getFloat()
    {
      return value == null ? 0f : value.floatValue();
    }

    @Override
    public long getLong()
    {
      return value == null ? 0L : value.longValue();
    }

    @Override
    public boolean isNull()
    {
      return value == null;
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return value;
    }

    @Override
    public Class<Object> classOfObject()
    {
      return Object.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("value", String.valueOf(value));
    }
  }
}
