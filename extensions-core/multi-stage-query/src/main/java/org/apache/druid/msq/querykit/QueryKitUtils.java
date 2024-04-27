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

package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.indexing.error.ColumnNameRestrictedFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.query.Query;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility methods for QueryKit.
 */
public class QueryKitUtils
{
  /**
   * Field in frames that stores the partition "boosting" value. Typically, it is used as the last element of a
   * partitioning key when generating segments. This is an incrementing number that helps split up otherwise too-large
   * partitions.
   */
  public static final String PARTITION_BOOST_COLUMN = "__boost";

  /**
   * Field in frames that stores the segment-granular timestamp.
   */
  public static final String SEGMENT_GRANULARITY_COLUMN = "__bucket";

  /**
   * Enables QueryKit-generated processors to understand which output column will be mapped to
   * {@link org.apache.druid.segment.column.ColumnHolder#TIME_COLUMN_NAME}. Necessary because {@link QueryKit}
   * does not get direct access to {@link ColumnMappings}.
   */
  public static final String CTX_TIME_COLUMN_NAME = "__timeColumn";

  public static Granularity getSegmentGranularityFromContext(
      final ObjectMapper objectMapper,
      @Nullable final Map<String, Object> context
  )
  {
    final Object o = context == null ? null : context.get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY);

    if (o instanceof String) {
      try {
        return objectMapper.readValue((String) o, Granularity.class);
      }
      catch (JsonProcessingException e) {
        throw new ISE("Invalid segment granularity [%s]", o);
      }
    } else if (o == null) {
      return Granularities.ALL;
    } else {
      throw new ISE("Invalid segment granularity [%s]", o);
    }
  }

  /**
   * Adds bucketing by {@link #SEGMENT_GRANULARITY_COLUMN} to a {@link ClusterBy} if needed.
   */
  public static ClusterBy clusterByWithSegmentGranularity(
      final ClusterBy clusterBy,
      final Granularity segmentGranularity
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return clusterBy;
    } else {
      final List<KeyColumn> newColumns = new ArrayList<>(clusterBy.getColumns().size() + 1);
      newColumns.add(new KeyColumn(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN, KeyOrder.ASCENDING));
      newColumns.addAll(clusterBy.getColumns());
      return new ClusterBy(newColumns, 1);
    }
  }

  /**
   * Verifies the {@link RowSignature} and throws an appropriate exception if it is invalid or uses restricted column
   * names
   */
  public static void verifyRowSignature(final RowSignature signature)
  {
    if (signature.contains(QueryKitUtils.PARTITION_BOOST_COLUMN)) {
      throw new MSQException(new ColumnNameRestrictedFault(QueryKitUtils.PARTITION_BOOST_COLUMN));
    } else if (signature.contains(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN)) {
      throw new MSQException(new ColumnNameRestrictedFault(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN));
    }
  }

  /**
   * Adds {@link #SEGMENT_GRANULARITY_COLUMN} to a {@link RowSignature} if needed. Signature should be verified prior
   * to calling this function to ensure that {@link #SEGMENT_GRANULARITY_COLUMN} is not passed in by the user
   */
  public static RowSignature signatureWithSegmentGranularity(
      final RowSignature signature,
      final Granularity segmentGranularity
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return signature;
    } else {
      return RowSignature.builder()
                         .addAll(signature)
                         .add(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN, ColumnType.LONG)
                         .build();
    }
  }

  /**
   * Returns a copy of "signature" with columns rearranged so the provided clusterByColumns appear as a prefix.
   * Throws an error if any of the clusterByColumns are not present in the input signature, or if any of their
   * types are unknown.
   */
  public static RowSignature sortableSignature(
      final RowSignature signature,
      final List<KeyColumn> clusterByColumns
  )
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (final KeyColumn columnName : clusterByColumns) {
      final Optional<ColumnType> columnType = signature.getColumnType(columnName.columnName());
      if (!columnType.isPresent()) {
        throw new IAE("Column [%s] not present in signature", columnName);
      }

      builder.add(columnName.columnName(), columnType.get());
    }

    final Set<String> clusterByColumnNames =
        clusterByColumns.stream().map(KeyColumn::columnName).collect(Collectors.toSet());

    for (int i = 0; i < signature.size(); i++) {
      final String columnName = signature.getColumnName(i);
      if (!clusterByColumnNames.contains(columnName)) {
        builder.add(columnName, signature.getColumnType(i).orElse(null));
      }
    }

    return builder.build();
  }

  /**
   * Returns a virtual column named {@link QueryKitUtils#SEGMENT_GRANULARITY_COLUMN} that computes a segment
   * granularity based on a particular time column. Returns null if no virtual column is needed because the
   * granularity is {@link Granularities#ALL}. Throws an exception if the provided granularity is not supported.
   *
   * @throws IllegalArgumentException if the provided granularity is not supported
   */
  @Nullable
  public static VirtualColumn makeSegmentGranularityVirtualColumn(final ObjectMapper jsonMapper, final Query<?> query)
  {
    final Granularity segmentGranularity =
        QueryKitUtils.getSegmentGranularityFromContext(jsonMapper, query.getContext());
    final String timeColumnName = query.context().getString(QueryKitUtils.CTX_TIME_COLUMN_NAME);

    if (timeColumnName == null || Granularities.ALL.equals(segmentGranularity)) {
      return null;
    }

    if (!(segmentGranularity instanceof PeriodGranularity)) {
      throw new IAE("Granularity [%s] is not supported", segmentGranularity);
    }

    final PeriodGranularity periodSegmentGranularity = (PeriodGranularity) segmentGranularity;

    if (periodSegmentGranularity.getOrigin() != null
        || !periodSegmentGranularity.getTimeZone().equals(DateTimeZone.UTC)) {
      throw new IAE("Granularity [%s] is not supported", segmentGranularity);
    }

    return new ExpressionVirtualColumn(
        QueryKitUtils.SEGMENT_GRANULARITY_COLUMN,
        StringUtils.format(
            "timestamp_floor(%s, %s)",
            CalciteSqlDialect.DEFAULT.quoteIdentifier(timeColumnName),
            Calcites.escapeStringLiteral((periodSegmentGranularity).getPeriod().toString())
        ),
        ColumnType.LONG,
        new ExprMacroTable(Collections.singletonList(new TimestampFloorExprMacro()))
    );
  }
}
