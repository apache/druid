/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.base.Preconditions;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;

public class ConstantColumnValueSelector<T> implements ColumnValueSelector<T>
{
  private long longValue;
  private float floatValue;
  private double doubleValue;

  @Nullable
  private T objectValue;

  private Class<T> objectClass;

  public ConstantColumnValueSelector(
      final long longValue,
      final float floatValue,
      final double doubleValue,
      @Nullable final T objectValue,
      final Class<T> objectClass
  )
  {
    this.longValue = longValue;
    this.floatValue = floatValue;
    this.doubleValue = doubleValue;
    this.objectValue = objectValue;
    this.objectClass = Preconditions.checkNotNull(objectClass, "objectClass");
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

  @Nullable
  @Override
  public T getObject()
  {
    return objectValue;
  }

  @Override
  public Class<T> classOfObject()
  {
    return objectClass;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    // Nothing here: objectValue is nullable but getObject is not @CalledFromHotLoop
  }
}
