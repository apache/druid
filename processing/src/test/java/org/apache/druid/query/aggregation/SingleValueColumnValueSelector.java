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

package org.apache.druid.query.aggregation;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class SingleValueColumnValueSelector<T> implements ColumnValueSelector<T>
{
  private final Class<T> valueClass;
  private final T value;

  public SingleValueColumnValueSelector(Class<T> valueClass, T value)
  {
    this.valueClass = valueClass;
    this.value = value;
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
  }

  @Override
  public boolean isNull()
  {
    return false;
  }

  @Nullable
  @Override
  public T getObject()
  {
    return value;
  }

  @Override
  public Class<? extends T> classOfObject()
  {
    return valueClass;
  }
}
