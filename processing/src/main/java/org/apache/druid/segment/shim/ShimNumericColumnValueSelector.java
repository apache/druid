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

package org.apache.druid.segment.shim;

import org.apache.druid.error.DruidException;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * Shim from {@link VectorValueSelector} to {@link ColumnValueSelector} for a {@link ShimCursor}.
 */
public class ShimNumericColumnValueSelector implements ColumnValueSelector<Object>
{
  private final ShimCursor cursor;
  private final ReadableVectorInspector vectorInspector;
  private final VectorValueSelector vectorSelector;
  private final Supplier<Object> getObjectSupplier;

  private double[] doubleVector;
  private float[] floatVector;
  private long[] longVector;
  private boolean[] nullVector;

  private int doubleId = ReadableVectorInspector.NULL_ID;
  private int floatId = ReadableVectorInspector.NULL_ID;
  private int longId = ReadableVectorInspector.NULL_ID;
  private int nullId = ReadableVectorInspector.NULL_ID;

  public ShimNumericColumnValueSelector(
      final ShimCursor cursor,
      final VectorValueSelector vectorSelector,
      final ValueType preferredType
  )
  {
    this.cursor = cursor;
    this.vectorInspector = cursor.vectorColumnSelectorFactory.getReadableVectorInspector();
    this.vectorSelector = vectorSelector;

    if (preferredType == ValueType.DOUBLE) {
      this.getObjectSupplier = () -> isNull() ? null : getDouble();
    } else if (preferredType == ValueType.FLOAT) {
      this.getObjectSupplier = () -> isNull() ? null : getFloat();
    } else if (preferredType == ValueType.LONG) {
      this.getObjectSupplier = () -> isNull() ? null : getLong();
    } else {
      throw DruidException.defensive("Unsupported preferredType[%s], must be numeric", preferredType);
    }
  }

  @Override
  public double getDouble()
  {
    populateDoubleVector();
    return doubleVector[cursor.currentIndexInVector];
  }

  @Override
  public float getFloat()
  {
    populateFloatVector();
    return floatVector[cursor.currentIndexInVector];
  }

  @Override
  public long getLong()
  {
    populateLongVector();
    return longVector[cursor.currentIndexInVector];
  }

  @Override
  public boolean isNull()
  {
    populateNullVector();
    return nullVector != null && nullVector[cursor.currentIndexInVector];
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return getObjectSupplier.get();
  }

  @Override
  public Class<?> classOfObject()
  {
    return Number.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // Don't bother.
  }

  private void populateDoubleVector()
  {
    final int id = vectorInspector.getId();
    if (id != doubleId) {
      doubleVector = vectorSelector.getDoubleVector();
      doubleId = id;
    }
  }

  private void populateFloatVector()
  {
    final int id = vectorInspector.getId();
    if (id != floatId) {
      floatVector = vectorSelector.getFloatVector();
      floatId = id;
    }
  }

  private void populateLongVector()
  {
    final int id = vectorInspector.getId();
    if (id != longId) {
      longVector = vectorSelector.getLongVector();
      longId = id;
    }
  }

  private void populateNullVector()
  {
    final int id = vectorInspector.getId();
    if (id != nullId) {
      nullVector = vectorSelector.getNullVector();
      nullId = id;
    }
  }
}
