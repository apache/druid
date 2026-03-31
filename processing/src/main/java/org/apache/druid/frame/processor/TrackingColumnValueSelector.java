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

package org.apache.druid.frame.processor;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * {@link ColumnValueSelector} that delegates to a selector obtained from a {@link ColumnSelectorFactory} supplier.
 * The delegate is refreshed when the supplier returns a different factory (by identity). This allows the selector
 * to remain valid across changes to the underlying data source, such as frame changes during merging or combining.
 */
public class TrackingColumnValueSelector implements ColumnValueSelector<Object>
{
  private final String columnName;
  private final Supplier<ColumnSelectorFactory> factorySupplier;
  private ColumnSelectorFactory delegateFactory;
  private ColumnValueSelector<?> delegate;

  public TrackingColumnValueSelector(final String columnName, final Supplier<ColumnSelectorFactory> factorySupplier)
  {
    this.columnName = columnName;
    this.factorySupplier = factorySupplier;
  }

  private ColumnValueSelector<?> delegate()
  {
    final ColumnSelectorFactory currentFactory = factorySupplier.get();
    //noinspection ObjectEquality
    if (currentFactory != delegateFactory) {
      delegateFactory = currentFactory;
      delegate = currentFactory.makeColumnValueSelector(columnName);
    }
    return delegate;
  }

  @Override
  public double getDouble()
  {
    return delegate().getDouble();
  }

  @Override
  public float getFloat()
  {
    return delegate().getFloat();
  }

  @Override
  public long getLong()
  {
    return delegate().getLong();
  }

  @Override
  public boolean isNull()
  {
    return delegate().isNull();
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return delegate().getObject();
  }

  @Override
  public Class<?> classOfObject()
  {
    return Object.class;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    // Do nothing.
  }
}
