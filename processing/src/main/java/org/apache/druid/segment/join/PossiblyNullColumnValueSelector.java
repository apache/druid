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

package org.apache.druid.segment.join;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.function.BooleanSupplier;

/**
 * A {@link ColumnValueSelector} that wraps a base selector but might also generate null values on demand. This
 * is used for "righty" joins (see {@link JoinType#isRighty()}), which may need to generate nulls on the left-hand side.
 */
public class PossiblyNullColumnValueSelector<T> implements ColumnValueSelector<T>
{
  private final ColumnValueSelector<T> baseSelector;
  private final BooleanSupplier beNull;
  @Nullable
  private final T nullValue;

  PossiblyNullColumnValueSelector(final ColumnValueSelector<T> baseSelector, final BooleanSupplier beNull)
  {
    this.baseSelector = baseSelector;
    this.beNull = beNull;
    this.nullValue = NullHandling.defaultValueForClass(baseSelector.classOfObject());
  }

  @Override
  public double getDouble()
  {
    return beNull.getAsBoolean() ? NullHandling.ZERO_DOUBLE : baseSelector.getDouble();
  }

  @Override
  public float getFloat()
  {
    return beNull.getAsBoolean() ? NullHandling.ZERO_FLOAT : baseSelector.getFloat();
  }

  @Override
  public long getLong()
  {
    return beNull.getAsBoolean() ? NullHandling.ZERO_LONG : baseSelector.getLong();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    baseSelector.inspectRuntimeShape(inspector);
    inspector.visit("beNull", beNull);
  }

  @Nullable
  @Override
  public T getObject()
  {
    return beNull.getAsBoolean() ? nullValue : baseSelector.getObject();
  }

  @Override
  public Class<? extends T> classOfObject()
  {
    return baseSelector.classOfObject();
  }

  @Override
  public boolean isNull()
  {
    return beNull.getAsBoolean() || baseSelector.isNull();
  }
}
