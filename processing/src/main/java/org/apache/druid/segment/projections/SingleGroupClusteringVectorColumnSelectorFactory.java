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

package org.apache.druid.segment.projections;

import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.ConstantVectorSelectors;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

/**
 * Vectorized counterpart of {@link SingleGroupClusteringColumnSelectorFactory}: the single-cluster-group fast path for
 * a clustered base-table segment. Non-clustering columns delegate directly to the per-group
 * {@link VectorColumnSelectorFactory}; clustering columns are surfaced as constant vector selectors.
 */
public class SingleGroupClusteringVectorColumnSelectorFactory implements VectorColumnSelectorFactory
{
  private final VectorColumnSelectorFactory delegate;
  private final RowSignature clusteringColumns;
  private final Object[] clusteringValues;

  public SingleGroupClusteringVectorColumnSelectorFactory(
      VectorColumnSelectorFactory delegate,
      RowSignature clusteringColumns,
      Object[] clusteringValues
  )
  {
    if (clusteringValues == null || clusteringValues.length != clusteringColumns.size()) {
      throw DruidException.defensive(
          "clusteringValues length [%s] must match clusteringColumns size [%s]",
          clusteringValues == null ? "null" : clusteringValues.length,
          clusteringColumns.size()
      );
    }
    this.delegate = delegate;
    this.clusteringColumns = clusteringColumns;
    this.clusteringValues = clusteringValues;
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
    final int idx = clusteringColumns.indexOf(dimensionSpec.getDimension());
    if (idx < 0) {
      return delegate.makeSingleValueDimensionSelector(dimensionSpec);
    }
    final Object raw = clusteringValues[idx];
    return ConstantVectorSelectors.singleValueDimensionVectorSelector(delegate.getReadableVectorInspector(), Evals.asString(raw));
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
  {
    if (clusteringColumns.indexOf(dimensionSpec.getDimension()) < 0) {
      return delegate.makeMultiValueDimensionSelector(dimensionSpec);
    }
    throw DruidException.defensive(
        "clustering column [%s] is not dictionary-encoded; no multi-value dimension vector selector",
        dimensionSpec.getDimension()
    );
  }

  @Override
  public VectorValueSelector makeValueSelector(String column)
  {
    final int idx = clusteringColumns.indexOf(column);
    if (idx < 0) {
      return delegate.makeValueSelector(column);
    }
    final Object raw = clusteringValues[idx];
    final Number number = (raw instanceof Number) ? (Number) raw : null;
    return ConstantVectorSelectors.vectorValueSelector(delegate.getReadableVectorInspector(), number);
  }

  @Override
  public VectorObjectSelector makeObjectSelector(String column)
  {
    final int idx = clusteringColumns.indexOf(column);
    if (idx < 0) {
      return delegate.makeObjectSelector(column);
    }
    return ConstantVectorSelectors.vectorObjectSelector(delegate.getReadableVectorInspector(), clusteringValues[idx]);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final int idx = clusteringColumns.indexOf(column);
    if (idx < 0) {
      return delegate.getColumnCapabilities(column);
    }
    final ColumnType type = clusteringColumns.getColumnType(idx).orElseThrow();
    if (type.is(ValueType.STRING)) {
      return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                                   .setDictionaryEncoded(true)
                                   .setDictionaryValuesSorted(true)
                                   .setDictionaryValuesUnique(true);
    }
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(type);
  }
}
