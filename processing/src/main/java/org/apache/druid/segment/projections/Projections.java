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
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
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
import java.util.HashMap;
import java.util.HashSet;
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
  public static <T> Projection<T> findMatchingProjection(
      CursorBuildSpec cursorBuildSpec,
      SortedSet<AggregateProjectionSpec> projections,
      PhysicalColumnChecker physicalChecker,
      Function<String, T> getRowSelector
  )
  {
    if (cursorBuildSpec.getQueryContext().getBoolean(QueryContexts.CTX_NO_PROJECTION, false)) {
      return null;
    }
    final String name = cursorBuildSpec.getQueryContext().getString(QueryContexts.CTX_USE_PROJECTION);

    if (cursorBuildSpec.isAggregate()) {
      for (AggregateProjectionSpec spec : projections) {
        if (name != null && !name.equals(spec.getName())) {
          continue;
        }
        final Map<String, String> rewriteColumns = new HashMap<>();
        if (spec.getTimeColumnName() != null) {
          rewriteColumns.put(spec.getTimeColumnName(), ColumnHolder.TIME_COLUMN_NAME);
        }
        final Set<VirtualColumn> referenced = new HashSet<>();
        if (spec.matches(cursorBuildSpec, referenced, rewriteColumns, physicalChecker)) {
          final CursorBuildSpec rewrittenBuildSpec =
              CursorBuildSpec.builder(cursorBuildSpec)
                             .setVirtualColumns(VirtualColumns.fromIterable(referenced))
                             .build();

          return new Projection<>(rewrittenBuildSpec, rewriteColumns, getRowSelector.apply(spec.getName()));
        }
      }
    }
    if (name != null) {
      throw InvalidInput.exception("Projection[%s] specified, but does not satisfy query", name);
    }
    return null;
  }

  public static String getProjectionSmooshV9FileName(AggregateProjectionSpec projectionSpec, String columnName)
  {
    return getProjectionSmooshV9Prefix(projectionSpec) + columnName;
  }

  public static String getProjectionSmooshV9Prefix(AggregateProjectionSpec projectionSpec)
  {
    return projectionSpec.getName() + "/";
  }

  /**
   * Returns true if column is defined in {@link AggregateProjectionSpec#getGroupingColumns()} OR if the column does not
   * exist in the base table. Part of determining if a projection can be used for a given {@link CursorBuildSpec},
   * 
   * @see AggregateProjectionSpec#matches(CursorBuildSpec, Set, Map, PhysicalColumnChecker)
   */
  @FunctionalInterface
  public interface PhysicalColumnChecker
  {
    boolean check(String projectionName, String columnName);
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
    public ColumnValueSelector<?> makeColumnValueSelector(
        ReadableOffset offset
    )
    {
      // todo (clint): i guess this is fine, kind of weird have to make an expreval tho
      return new ConstantExprEvalSelector(ExprEval.ofLong(constant));
    }

    @Override
    public VectorValueSelector makeVectorValueSelector(
        ReadableVectorOffset offset
    )
    {
      return ConstantVectorSelectors.vectorValueSelector(
          offset,
          constant
      );
    }
  }
}
