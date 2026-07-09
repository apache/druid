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
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantExprEvalSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

/**
 * Single-cluster-group counterpart of {@link ClusteringColumnSelectorFactory}, used when a query prunes a clustered
 * {@link org.apache.druid.segment.QueryableIndexCursorFactory}). Non-clustering columns are delegated directly to
 * the per-group factory; clustering columns, which are not physically stored in the per-group data, are surfaced as
 * constants from the group's clustering tuple.
 */
public class SingleGroupClusteringColumnSelectorFactory implements ColumnSelectorFactory
{
  private final ColumnSelectorFactory delegate;
  private final RowSignature clusteringColumns;
  private final Object[] clusteringValues;

  public SingleGroupClusteringColumnSelectorFactory(
      ColumnSelectorFactory delegate,
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
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    final int idx = clusteringColumns.indexOf(dimensionSpec.getDimension());
    if (idx < 0) {
      return delegate.makeDimensionSelector(dimensionSpec);
    }
    final Object raw = clusteringValues[idx];
    return DimensionSelector.constant(Evals.asString(raw), dimensionSpec.getExtractionFn());
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(String columnName)
  {
    final int idx = clusteringColumns.indexOf(columnName);
    if (idx < 0) {
      return delegate.makeColumnValueSelector(columnName);
    }
    return new ConstantClusteringValueSelector(clusteringColumns.getColumnType(idx).orElseThrow(), clusteringValues[idx]);
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

  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return delegate.getRowIdSupplier();
  }

  /**
   * Constant value selector for a clustering column's group-constant typed value.
   */
  private static final class ConstantClusteringValueSelector implements ColumnValueSelector<Object>
  {
    private final ConstantExprEvalSelector inner;

    private ConstantClusteringValueSelector(ColumnType columnType, Object value)
    {
      this.inner = new ConstantExprEvalSelector(ExprEval.ofType(ExpressionType.fromColumnTypeStrict(columnType), value));
    }

    @Override
    public double getDouble()
    {
      return inner.getDouble();
    }

    @Override
    public float getFloat()
    {
      return inner.getFloat();
    }

    @Override
    public long getLong()
    {
      return inner.getLong();
    }

    @Override
    public boolean isNull()
    {
      return inner.isNull();
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return inner.getObject().value();
    }

    @Override
    public Class<Object> classOfObject()
    {
      return Object.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inner.inspectRuntimeShape(inspector);
    }
  }
}
