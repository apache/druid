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

import com.google.common.base.Preconditions;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;

/**
 * {@link ColumnSelectorFactory} which can create selectors for both virtual and non-virtual columns
 */
public class VirtualizedColumnSelectorFactory extends VirtualizedColumnInspector implements ColumnSelectorFactory
{
  private final ColumnSelectorFactory baseFactory;

  public VirtualizedColumnSelectorFactory(
      ColumnSelectorFactory baseFactory,
      VirtualColumns virtualColumns
  )
  {
    super(baseFactory, virtualColumns);
    this.baseFactory = Preconditions.checkNotNull(baseFactory, "baseFactory");
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    if (virtualColumns.exists(dimensionSpec.getDimension())) {
      return virtualColumns.makeDimensionSelector(dimensionSpec, this);
    } else {
      return baseFactory.makeDimensionSelector(dimensionSpec);
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    if (virtualColumns.exists(columnName)) {
      return virtualColumns.makeColumnValueSelector(columnName, this);
    } else {
      return baseFactory.makeColumnValueSelector(columnName);
    }
  }

  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return baseFactory.getRowIdSupplier();
  }
}
