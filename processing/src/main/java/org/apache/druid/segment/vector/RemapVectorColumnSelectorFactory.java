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

package org.apache.druid.segment.vector;

import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.DeferExpressionDimensions;
import org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.Map;

public class RemapVectorColumnSelectorFactory implements VectorColumnSelectorFactory
{
  private final VectorColumnSelectorFactory delegate;
  private final Map<String, String> remap;

  public RemapVectorColumnSelectorFactory(VectorColumnSelectorFactory delegate, Map<String, String> remap)
  {
    this.delegate = delegate;
    this.remap = remap;
  }

  @Override
  public ReadableVectorInspector getReadableVectorInspector()
  {
    return delegate.getReadableVectorInspector();
  }

  @Override
  public int getMaxVectorSize()
  {
    return delegate.getMaxVectorSize();
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
  {
    DimensionSpec remapDimensionSpec = dimensionSpec.withDimension(remap.getOrDefault(dimensionSpec.getDimension(), dimensionSpec.getDimension()));
    return delegate.makeSingleValueDimensionSelector(remapDimensionSpec);
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
  {
    DimensionSpec remapDimensionSpec = dimensionSpec.withDimension(remap.getOrDefault(dimensionSpec.getDimension(), dimensionSpec.getDimension()));
    return delegate.makeMultiValueDimensionSelector(remapDimensionSpec);
  }

  @Override
  public VectorValueSelector makeValueSelector(String column)
  {
    return delegate.makeValueSelector(remap.getOrDefault(column, column));
  }

  @Override
  public VectorObjectSelector makeObjectSelector(String column)
  {
    return delegate.makeObjectSelector(remap.getOrDefault(column, column));
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return delegate.getColumnCapabilities(remap.getOrDefault(column, column));
  }

  @Nullable
  @Override
  public ExpressionType getType(String name)
  {
    return delegate.getType(remap.getOrDefault(name, name));
  }

  @Override
  public GroupByVectorColumnSelector makeGroupByVectorColumnSelector(
      String column,
      DeferExpressionDimensions deferExpressionDimensions
  )
  {
    return delegate.makeGroupByVectorColumnSelector(remap.getOrDefault(column, column), deferExpressionDimensions);
  }
}
