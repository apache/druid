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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantExprEvalSelector;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.CapabilitiesBasedFormat;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ConstantVectorSelectors;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

public class Projections
{
  public static Supplier<ColumnHolder> makeConstantTimeSupplier(int numRows, long constant)
  {
    return Suppliers.memoize(
        () -> new ColumnBuilder().setNumericColumnSupplier(() -> new ConstantTimeColumn(numRows, constant))
                                 .setColumnFormat(
                                     new CapabilitiesBasedFormat(
                                         ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG)
                                     )
                                 )
                                 .setType(ColumnType.LONG)
                                 .setHasNulls(false)
                                 .build()
    );
  }

  @Nullable
  public static <T> QueryableProjection<T> findMatchingProjection(
      CursorBuildSpec cursorBuildSpec,
      SortedSet<AggregateProjectionMetadata> projections,
      PhysicalColumnChecker physicalChecker,
      Function<String, T> getRowSelector
  )
  {
    if (cursorBuildSpec.getQueryContext().getBoolean(QueryContexts.NO_PROJECTIONS, false)) {
      return null;
    }
    final String name = cursorBuildSpec.getQueryContext().getString(QueryContexts.USE_PROJECTION);

    if (cursorBuildSpec.isAggregate()) {
      for (AggregateProjectionMetadata spec : projections) {
        if (name != null && !name.equals(spec.getSchema().getName())) {
          continue;
        }
        final ProjectionMatch match = spec.getSchema().matches(cursorBuildSpec, physicalChecker);
        if (match != null) {
          if (cursorBuildSpec.getQueryMetrics() != null) {
            cursorBuildSpec.getQueryMetrics().projection(spec.getSchema().getName());
          }
          return new QueryableProjection<>(
              match.getCursorBuildSpec(),
              match.getRemapColumns(),
              getRowSelector.apply(spec.getSchema().getName())
          );
        }
      }
    }
    if (name != null) {
      throw InvalidInput.exception("Projection[%s] specified, but does not satisfy query", name);
    }
    if (cursorBuildSpec.getQueryContext().getBoolean(QueryContexts.FORCE_PROJECTION, false)) {
      throw InvalidInput.exception("Force projections specified, but none satisfy query");
    }
    return null;
  }

  public static String getProjectionSmooshV9FileName(AggregateProjectionMetadata projectionSpec, String columnName)
  {
    return getProjectionSmooshV9Prefix(projectionSpec) + columnName;
  }

  public static String getProjectionSmooshV9Prefix(AggregateProjectionMetadata projectionSpec)
  {
    return projectionSpec.getSchema().getName() + "/";
  }

  /**
   * Returns true if column is defined in {@link AggregateProjectionSpec#getGroupingColumns()} OR if the column does not
   * exist in the base table. Part of determining if a projection can be used for a given {@link CursorBuildSpec},
   * 
   * @see AggregateProjectionMetadata.Schema#matches(CursorBuildSpec, PhysicalColumnChecker)
   */
  @FunctionalInterface
  public interface PhysicalColumnChecker
  {
    boolean check(String projectionName, String columnName);
  }

  public static final class ProjectionMatch
  {
    private final CursorBuildSpec cursorBuildSpec;
    private final Map<String, String> remapColumns;

    public ProjectionMatch(CursorBuildSpec cursorBuildSpec, Map<String, String> remapColumns)
    {
      this.cursorBuildSpec = cursorBuildSpec;
      this.remapColumns = remapColumns;
    }

    public CursorBuildSpec getCursorBuildSpec()
    {
      return cursorBuildSpec;
    }

    public Map<String, String> getRemapColumns()
    {
      return remapColumns;
    }
  }

  public static final class ProjectionMatchBuilder
  {
    private final Set<String> referencedPhysicalColumns;
    private final Set<VirtualColumn> referencedVirtualColumns;
    private final Map<String, String> remapColumns;
    private final List<AggregatorFactory> combiningFactories;

    public ProjectionMatchBuilder()
    {
      this.referencedPhysicalColumns = new HashSet<>();
      this.referencedVirtualColumns = new HashSet<>();
      this.remapColumns = new HashMap<>();
      this.combiningFactories = new ArrayList<>();
    }

    /**
     * Map a query column name to a projection column name
     */
    public ProjectionMatchBuilder remapColumn(String queryColumn, String projectionColumn)
    {
      remapColumns.put(queryColumn, projectionColumn);
      return this;
    }

    /**
     * Add a projection physical column, which will later be added to {@link ProjectionMatch#getCursorBuildSpec()} if
     * the projection matches
     */
    public ProjectionMatchBuilder addReferencedPhysicalColumn(String column)
    {
      referencedPhysicalColumns.add(column);
      return this;
    }

    /**
     * Add a query virtual column that can use projection physical columns as inputs to the match builder, which will
     * later be added to {@link ProjectionMatch#getCursorBuildSpec()} if the projection matches
     */
    public ProjectionMatchBuilder addReferenceedVirtualColumn(VirtualColumn virtualColumn)
    {
      referencedVirtualColumns.add(virtualColumn);
      return this;
    }

    /**
     * Add a query {@link AggregatorFactory#substituteCombiningFactory(AggregatorFactory)} which can combine the inputs
     * of a selector created by a projection {@link AggregatorFactory}
     *
     */
    public ProjectionMatchBuilder addPreAggregatedAggregator(AggregatorFactory aggregator)
    {
      combiningFactories.add(aggregator);
      return this;
    }

    public ProjectionMatch build(CursorBuildSpec queryCursorBuildSpec)
    {
      return new ProjectionMatch(
          CursorBuildSpec.builder(queryCursorBuildSpec)
                         .setPhysicalColumns(referencedPhysicalColumns)
                         .setVirtualColumns(VirtualColumns.fromIterable(referencedVirtualColumns))
                         .setAggregators(combiningFactories)
                         .build(),
          remapColumns
      );
    }
  }

  private static class ConstantTimeColumn implements NumericColumn
  {
    private final int numRows;
    private final long constant;

    private ConstantTimeColumn(int numRows, long constant)
    {
      this.numRows = numRows;
      this.constant = constant;
    }

    @Override
    public int length()
    {
      return numRows;
    }

    @Override
    public long getLongSingleValueRow(int rowNum)
    {
      return constant;
    }

    @Override
    public void close()
    {
      // nothing to close
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
    {
      return new ConstantExprEvalSelector(ExprEval.ofLong(constant));
    }

    @Override
    public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
    {
      return ConstantVectorSelectors.vectorValueSelector(offset, constant);
    }
  }
}
