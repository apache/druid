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

package org.apache.druid.segment;

import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.Map;

public class RemapColumnSelectorFactory implements ColumnSelectorFactory
{
  private final ColumnSelectorFactory delegate;
  private final Map<String, String> remap;

  public RemapColumnSelectorFactory(ColumnSelectorFactory delegate, Map<String, String> remap)
  {
    this.delegate = delegate;
    this.remap = remap;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    DimensionSpec remapDimensionSpec = dimensionSpec.withDimension(remap.getOrDefault(dimensionSpec.getDimension(), dimensionSpec.getDimension()));
    return delegate.makeDimensionSelector(remapDimensionSpec);
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(String columnName)
  {
    return delegate.makeColumnValueSelector(remap.getOrDefault(columnName, columnName));
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return delegate.getColumnCapabilities(remap.getOrDefault(column, column));
  }

  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return delegate.getRowIdSupplier();
  }

  @Nullable
  @Override
  public ExpressionType getType(String name)
  {
    return delegate.getType(remap.getOrDefault(name, name));
  }
}
