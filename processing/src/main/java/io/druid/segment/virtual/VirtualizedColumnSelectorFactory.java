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

package io.druid.segment.virtual;

import com.google.common.base.Preconditions;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;

public class VirtualizedColumnSelectorFactory implements ColumnSelectorFactory
{
  private final ColumnSelectorFactory baseFactory;
  private final VirtualColumns virtualColumns;

  public VirtualizedColumnSelectorFactory(
      ColumnSelectorFactory baseFactory,
      VirtualColumns virtualColumns
  )
  {
    this.baseFactory = Preconditions.checkNotNull(baseFactory, "baseFactory");
    this.virtualColumns = Preconditions.checkNotNull(virtualColumns, "virtualColumns");
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    if (virtualColumns.exists(dimensionSpec.getDimension())) {
      return virtualColumns.makeDimensionSelector(dimensionSpec, baseFactory);
    } else {
      return baseFactory.makeDimensionSelector(dimensionSpec);
    }
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeFloatColumnSelector(columnName, baseFactory);
    } else {
      return baseFactory.makeFloatColumnSelector(columnName);
    }
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeLongColumnSelector(columnName, baseFactory);
    } else {
      return baseFactory.makeLongColumnSelector(columnName);
    }
  }

  @Override
  public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeDoubleColumnSelector(columnName, baseFactory);
    } else {
      return baseFactory.makeDoubleColumnSelector(columnName);
    }
  }

  @Nullable
  @Override
  public ObjectColumnSelector makeObjectColumnSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeObjectColumnSelector(columnName, baseFactory);
    } else {
      return baseFactory.makeObjectColumnSelector(columnName);
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.getColumnCapabilities(columnName);
    } else {
      return baseFactory.getColumnCapabilities(columnName);
    }
  }
}
