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

package org.apache.druid.msq.querykit.groupby;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.SimpleLongAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Utility methods for detecting and optimizing GroupBy queries that are effectively time boundary queries:
 * no dimensions, {@link Granularities#ALL}, and only {@code MIN(__time)} / {@code MAX(__time)} aggregators.
 */
public class GroupByTimeBoundaryUtils
{
  /**
   * Returns true if the query is a "time boundary" GroupBy: no dimensions, {@link Granularities#ALL},
   * no filter, at least one aggregator, and every aggregator is {@link LongMinAggregatorFactory} or
   * {@link LongMaxAggregatorFactory} on {@link ColumnHolder#TIME_COLUMN_NAME}.
   */
  public static boolean isTimeBoundaryQuery(final GroupByQuery query)
  {
    if (!query.getDimensions().isEmpty()) {
      return false;
    }

    if (!Granularities.ALL.equals(query.getGranularity())) {
      return false;
    }

    if (query.getDimFilter() != null) {
      return false;
    }

    final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();

    if (aggregatorSpecs.isEmpty()) {
      return false;
    }

    for (final AggregatorFactory agg : aggregatorSpecs) {
      if (!isTimeBoundaryAggregator(agg)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns true if the query needs the minimum time (has at least one {@link LongMinAggregatorFactory}
   * on {@link ColumnHolder#TIME_COLUMN_NAME}).
   */
  public static boolean needsMinTime(final GroupByQuery query)
  {
    for (final AggregatorFactory agg : query.getAggregatorSpecs()) {
      if (isTimeBoundaryAggregator(agg) && agg instanceof LongMinAggregatorFactory) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the query needs the maximum time (has at least one {@link LongMaxAggregatorFactory}
   * on {@link ColumnHolder#TIME_COLUMN_NAME}).
   */
  public static boolean needsMaxTime(final GroupByQuery query)
  {
    for (final AggregatorFactory agg : query.getAggregatorSpecs()) {
      if (isTimeBoundaryAggregator(agg) && agg instanceof LongMaxAggregatorFactory) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the {@link TimeBoundaryInspector} can be used to answer the query without scanning data.
   * Requires that the query is a time boundary query, the inspector is non-null and exact, and that the
   * descriptor's interval fully contains the inspector's min/max interval.
   */
  public static boolean canUseTimeBoundaryInspector(
      final GroupByQuery query,
      @Nullable final TimeBoundaryInspector tbi,
      final SegmentDescriptor descriptor
  )
  {
    return isTimeBoundaryQuery(query)
           && tbi != null
           && tbi.isMinMaxExact()
           && descriptor.getInterval().contains(tbi.getMinMaxInterval());
  }

  /**
   * Constructs a {@link ResultRow} from the time boundary inspector, filling each aggregator position
   * with the appropriate min or max time.
   */
  public static ResultRow computeTimeBoundaryResult(final GroupByQuery query, final TimeBoundaryInspector tbi)
  {
    final int size = query.getResultRowSizeWithoutPostAggregators();
    final ResultRow row = ResultRow.create(size);
    final int aggStart = query.getResultRowAggregatorStart();
    final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();

    for (int i = 0; i < aggregatorSpecs.size(); i++) {
      final AggregatorFactory agg = aggregatorSpecs.get(i);

      if (agg instanceof LongMinAggregatorFactory) {
        row.set(aggStart + i, tbi.getMinTime().getMillis());
      } else if (agg instanceof LongMaxAggregatorFactory) {
        row.set(aggStart + i, tbi.getMaxTime().getMillis());
      }
    }

    return row;
  }

  private static boolean isTimeBoundaryAggregator(final AggregatorFactory agg)
  {
    return (agg instanceof LongMinAggregatorFactory || agg instanceof LongMaxAggregatorFactory)
           && ColumnHolder.TIME_COLUMN_NAME.equals(((SimpleLongAggregatorFactory) agg).getFieldName());
  }
}
