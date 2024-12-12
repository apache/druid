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

package org.apache.druid.java.util.common.granularity;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Query;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * This class was created b/c sometimes static initializers of a class that use a subclass can deadlock.
 * See: #2979, #3979
 */
public class Granularities
{
  public static final Granularity SECOND = GranularityType.SECOND.getDefaultGranularity();
  public static final Granularity MINUTE = GranularityType.MINUTE.getDefaultGranularity();
  public static final Granularity FIVE_MINUTE = GranularityType.FIVE_MINUTE.getDefaultGranularity();
  public static final Granularity TEN_MINUTE = GranularityType.TEN_MINUTE.getDefaultGranularity();
  public static final Granularity FIFTEEN_MINUTE = GranularityType.FIFTEEN_MINUTE.getDefaultGranularity();
  public static final Granularity THIRTY_MINUTE = GranularityType.THIRTY_MINUTE.getDefaultGranularity();
  public static final Granularity HOUR = GranularityType.HOUR.getDefaultGranularity();
  public static final Granularity SIX_HOUR = GranularityType.SIX_HOUR.getDefaultGranularity();
  public static final Granularity EIGHT_HOUR = GranularityType.EIGHT_HOUR.getDefaultGranularity();
  public static final Granularity DAY = GranularityType.DAY.getDefaultGranularity();
  public static final Granularity WEEK = GranularityType.WEEK.getDefaultGranularity();
  public static final Granularity MONTH = GranularityType.MONTH.getDefaultGranularity();
  public static final Granularity QUARTER = GranularityType.QUARTER.getDefaultGranularity();
  public static final Granularity YEAR = GranularityType.YEAR.getDefaultGranularity();
  public static final Granularity ALL = GranularityType.ALL.getDefaultGranularity();
  public static final Granularity NONE = GranularityType.NONE.getDefaultGranularity();

  public static final String GRANULARITY_VIRTUAL_COLUMN_NAME = "__virtualGranularity";

  public static Granularity nullToAll(Granularity granularity)
  {
    return granularity == null ? Granularities.ALL : granularity;
  }

  /**
   * Decorates {@link CursorBuildSpec} with a grouping column and virtual column equivalent to the {@link Granularity}
   * for a {@link Query}, if that query has granularity other than {@link Granularities#ALL}. If the query has 'ALL'
   * granularity, the {@link CursorBuildSpec} will be returned as is.
   * The reason for this is that a query with a granularity that is not 'ALL' is effectively an additional grouping on
   * TIME_FLOOR(__time, granularity), so by adding this to the {@link CursorBuildSpec} allows a
   * {@link org.apache.druid.segment.CursorHolder} to potentially specialize any {@link org.apache.druid.segment.Cursor}
   * or {@link org.apache.druid.segment.vector.VectorCursor} it provides by using pre-aggregated data at the closest
   * matching granularity if available.
   *
   * @see #toVirtualColumn(Granularity, String) for how {@link Granularity} is translated into a {@link VirtualColumn}
   */
  public static CursorBuildSpec decorateCursorBuildSpec(Query<?> query, CursorBuildSpec buildSpec)
  {
    // ALL granularity
    if (ALL.equals(query.getGranularity())) {
      return buildSpec;
    }
    // For any other granularity, we add a grouping on TIME_FLOOR(__time, granularity) or __time itself
    final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    final Set<String> requiredColumns = query.getRequiredColumns();
    if (requiredColumns != null) {
      builder.addAll(requiredColumns);
    }
    builder.addAll(query.getVirtualColumns().getColumnNames());
    final Set<String> columnNamesForConflictResolution = builder.build();
    String virtualColumnName = GRANULARITY_VIRTUAL_COLUMN_NAME;
    int ctr = 0;
    while (columnNamesForConflictResolution.contains(virtualColumnName)) {
      virtualColumnName = virtualColumnName + ctr++;
    }

    final VirtualColumn granularityVirtual = toVirtualColumn(query.getGranularity(), virtualColumnName);
    // granularityVirtual will not be null because we have already filtered out ALL granularity, but check just in case
    Preconditions.checkNotNull(
        granularityVirtual,
        "Granularity virtual column is null for granularity[%s]",
        query.getGranularity()
    );

    final VirtualColumns virtualColumns = VirtualColumns.fromIterable(
        Iterables.concat(
            Collections.singletonList(granularityVirtual),
            () -> Arrays.stream(buildSpec.getVirtualColumns().getVirtualColumns()).iterator()
        )
    );
    final ImmutableList.Builder<String> groupingColumnsBuilder = ImmutableList.builder();
    groupingColumnsBuilder.add(granularityVirtual.getOutputName());
    if (buildSpec.getGroupingColumns() != null) {
      groupingColumnsBuilder.addAll(buildSpec.getGroupingColumns());
    }
    return CursorBuildSpec.builder(buildSpec)
                          .setVirtualColumns(virtualColumns)
                          .setGroupingColumns(groupingColumnsBuilder.build())
                          .build();
  }

  /**
   * Translates a {@link Granularity} to a {@link ExpressionVirtualColumn} on {@link ColumnHolder#TIME_COLUMN_NAME} of
   * the equivalent grouping column. If granularity is {@link #ALL}, this method returns null since we are not grouping
   * on time. If granularity is a {@link PeriodGranularity} with UTC timezone and no origin, this method returns a
   * virtual column with {@link TimestampFloorExprMacro.TimestampFloorExpr} of the specified period. If granularity is
   * {@link #NONE}, or any other kind of granularity (duration, period with non-utc timezone or origin) this method
   * returns a virtual column with {@link org.apache.druid.math.expr.IdentifierExpr} specifying
   * {@link ColumnHolder#TIME_COLUMN_NAME} directly.
   */
  @Nullable
  public static ExpressionVirtualColumn toVirtualColumn(Granularity granularity, String virtualColumnName)
  {
    if (ALL.equals(granularity)) {
      return null;
    }
    final String expression;
    if (NONE.equals(granularity) || granularity instanceof DurationGranularity) {
      expression = ColumnHolder.TIME_COLUMN_NAME;
    } else {
      PeriodGranularity period = (PeriodGranularity) granularity;
      if (!ISOChronology.getInstanceUTC().getZone().equals(period.getTimeZone()) || period.getOrigin() != null) {
        expression = ColumnHolder.TIME_COLUMN_NAME;
      } else {
        expression = TimestampFloorExprMacro.forQueryGranularity(period.getPeriod());
      }
    }

    return new ExpressionVirtualColumn(
        virtualColumnName,
        expression,
        ColumnType.LONG,
        ExprMacroTable.granularity()
    );
  }

  /**
   * Converts a virtual column with a single input time column into a {@link Granularity} if it is a
   * {@link TimestampFloorExprMacro.TimestampFloorExpr}.
   * <p>
   * IMPORTANT - this method DOES NOT VERIFY that the virtual column has a single input that is a time column
   * ({@link ColumnHolder#TIME_COLUMN_NAME} or equivalent projection time column as defined by
   * {@link AggregateProjectionMetadata.Schema#getTimeColumnName()}). Callers must verify this externally before
   * calling this method by examining {@link VirtualColumn#requiredColumns()}.
   * <p>
   * This method also does not handle other time expressions, or if the virtual column is just an identifier for a
   * time column
   */
  @Nullable
  public static Granularity fromVirtualColumn(VirtualColumn virtualColumn)
  {
    if (virtualColumn instanceof ExpressionVirtualColumn) {
      final ExpressionVirtualColumn expressionVirtualColumn = (ExpressionVirtualColumn) virtualColumn;
      final Expr expr = expressionVirtualColumn.getParsedExpression().get();
      if (expr instanceof TimestampFloorExprMacro.TimestampFloorExpr) {
        final TimestampFloorExprMacro.TimestampFloorExpr gran = (TimestampFloorExprMacro.TimestampFloorExpr) expr;
        if (gran.getArg().getBindingIfIdentifier() != null) {
          return gran.getGranularity();
        }
      }
    }
    return null;
  }
}
