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

package org.apache.druid.data.input.impl;

import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link BaseTableProjectionSpec} for a table that aggregates the rows it stores, and so has metric columns as well
 * as the columns it groups by (and how to group them).
 */
public interface RollupBaseTableProjectionSpec extends BaseTableProjectionSpec
{
  @Override
  default boolean isRollup()
  {
    return true;
  }

  /**
   * The columns this spec declares to be multi-value strings, read from the
   * {@link #MULTI_VALUE_VIRTUAL_COLUMN_PREFIX} carriers in {@link #getVirtualColumns()}.
   */
  default Set<String> getMultiValuedColumns()
  {
    final Set<String> columns = new HashSet<>();
    for (VirtualColumn virtualColumn : getVirtualColumns().getVirtualColumns()) {
      final String column = multiValueColumnFrom(virtualColumn);
      if (column != null) {
        columns.add(column);
      }
    }
    return columns;
  }

  /**
   * Returns a copy of this spec with the given aggregators appended to those it already declares, the metric-column
   * counterpart of {@link #withAdditionalColumns}. Each aggregator must combine its own output, which is what the
   * aggregators recovered from existing segments already do.
   */
  RollupBaseTableProjectionSpec withAdditionalMetrics(@Nullable AggregatorFactory[] additionalMetrics);

  /**
   * The carrier declaring a {@code column} to be a multi-valued string.
   */
  static ExpressionVirtualColumn toMultiValueVirtualColumn(String column)
  {
    return new ExpressionVirtualColumn(
        MULTI_VALUE_VIRTUAL_COLUMN_PREFIX + column,
        StringUtils.format("mv_to_array(%s)", Parser.identifier(column).stringify()),
        ColumnType.STRING_ARRAY,
        ExprMacroTable.nil()
    );
  }

  /**
   * The column a multi-value carrier references, or null if {@code virtualColumn} is not one. Only the name is read, so
   * use {@link #validateMultiValue} to validate it is the correct shape.
   */
  @Nullable
  static String multiValueColumnFrom(VirtualColumn virtualColumn)
  {
    final String outputName = virtualColumn.getOutputName();
    if (!outputName.startsWith(MULTI_VALUE_VIRTUAL_COLUMN_PREFIX)) {
      return null;
    }
    final String column = outputName.substring(MULTI_VALUE_VIRTUAL_COLUMN_PREFIX.length());
    return column.isEmpty() ? null : column;
  }

  /**
   * Validate rollup layout virtual column carriers: query-granularity named
   * {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} and any multi-value columns starting with
   * {@link #MULTI_VALUE_VIRTUAL_COLUMN_PREFIX}.
   */
  static void validateGroupingCarrierVirtualColumns(
      VirtualColumns virtualColumns,
      List<DimensionSchema> groupingColumns,
      String typeName
  )
  {
    for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      final String outputName = virtualColumn.getOutputName();
      if (Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME.equals(outputName)) {
        BaseTableProjectionSpec.validateGranularity(virtualColumn, typeName);
      } else if (outputName.startsWith(MULTI_VALUE_VIRTUAL_COLUMN_PREFIX)) {
        validateMultiValue(virtualColumn, groupingColumns, typeName);
      } else {
        throw InvalidInput.exception(
            "virtual column [%s] is not supported: a [%s] base table stores only declared columns and does not"
            + " materialize virtual columns; use a transformSpec to compute columns at ingest, the [%s] virtual"
            + " column to carry query granularity, or a [%s] virtual column to declare a grouping column"
            + " multi-valued",
            outputName,
            typeName,
            Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME,
            MULTI_VALUE_VIRTUAL_COLUMN_PREFIX + "<column>"
        );
      }
    }
  }

  /**
   * Validates a {@link #MULTI_VALUE_VIRTUAL_COLUMN_PREFIX} carrier: it must name a declared grouping column that
   * could hold multiple values, and be exactly the form {@link #toMultiValueVirtualColumn} builds.
   */
  static void validateMultiValue(
      VirtualColumn multiValueColumn,
      List<DimensionSchema> groupingColumns,
      String typeName
  )
  {
    final String column = multiValueColumnFrom(multiValueColumn);
    if (column == null) {
      throw InvalidInput.exception(
          "virtual column [%s] does not name the column it declares multi-valued",
          multiValueColumn.getOutputName()
      );
    }
    DimensionSchema declared = null;
    for (DimensionSchema groupingColumn : groupingColumns) {
      if (groupingColumn.getName().equals(column)) {
        declared = groupingColumn;
        break;
      }
    }
    if (declared == null) {
      throw InvalidInput.exception(
          "virtual column [%s] declares [%s] multi-valued, but a [%s] base table does not group by that column",
          multiValueColumn.getOutputName(),
          column,
          typeName
      );
    }
    if (!declared.canBeMultiValued()) {
      throw InvalidInput.exception(
          "column [%s] is declared as [%s], which holds one value per row and cannot be declared multi-valued",
          column,
          declared.getColumnType()
      );
    }
    if (!toMultiValueVirtualColumn(column).equals(multiValueColumn)) {
      throw InvalidInput.exception(
          "virtual column [%s] must be [%s] over [%s] alone",
          multiValueColumn.getOutputName(),
          "mv_to_array",
          column
      );
    }
  }
}
