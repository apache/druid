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

package org.apache.druid.query.timeboundary;

import com.google.inject.Inject;
import it.unimi.dsi.fastutil.Pair;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.Order;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.Result;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 *
 */
public class TimeBoundaryQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeBoundaryResultValue>, TimeBoundaryQuery>
{
  private static final TimeBoundaryQueryQueryToolChest TOOL_CHEST = new TimeBoundaryQueryQueryToolChest();
  private final QueryWatcher queryWatcher;

  @Inject
  public TimeBoundaryQueryRunnerFactory(QueryWatcher queryWatcher)
  {
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> createRunner(final Segment segment)
  {
    return new TimeBoundaryQueryRunner(segment);
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> mergeRunners(
      QueryProcessingPool queryProcessingPool,
      Iterable<QueryRunner<Result<TimeBoundaryResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(queryProcessingPool, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Result<TimeBoundaryResultValue>, TimeBoundaryQuery> getToolchest()
  {
    return TOOL_CHEST;
  }

  private static class TimeBoundaryQueryRunner implements QueryRunner<Result<TimeBoundaryResultValue>>
  {
    private final CursorFactory cursorFactory;
    private final Interval dataInterval;
    @Nullable
    private final TimeBoundaryInspector timeBoundaryInspector;

    public TimeBoundaryQueryRunner(Segment segment)
    {
      this.cursorFactory = segment.asCursorFactory();
      this.dataInterval = segment.getDataInterval();
      this.timeBoundaryInspector = segment.as(TimeBoundaryInspector.class);
    }

    @Override
    public Sequence<Result<TimeBoundaryResultValue>> run(
        final QueryPlus<Result<TimeBoundaryResultValue>> queryPlus,
        final ResponseContext responseContext
    )
    {
      Query<Result<TimeBoundaryResultValue>> input = queryPlus.getQuery();
      if (!(input instanceof TimeBoundaryQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeBoundaryQuery.class);
      }

      final TimeBoundaryQuery query = (TimeBoundaryQuery) input;

      return new BaseSequence<>(
          new BaseSequence.IteratorMaker<>()
          {
            @Override
            public Iterator<Result<TimeBoundaryResultValue>> make()
            {
              if (cursorFactory == null) {
                throw new ISE(
                    "Null cursor factory found. Probably trying to issue a query against a segment being memory unmapped."
                );
              }

              DateTime minTime = null;
              DateTime maxTime = null;

              if (canUseTimeBoundaryInspector(query, timeBoundaryInspector)) {
                if (query.needsMinTime()) {
                  minTime = timeBoundaryInspector.getMinTime();
                }

                if (query.needsMaxTime()) {
                  maxTime = timeBoundaryInspector.getMaxTime();
                }
              } else {
                final Pair<DateTime, DateTime> timeBoundary = getTimeBoundary(query, cursorFactory);
                minTime = timeBoundary.left();
                maxTime = timeBoundary.right();
              }

              return query.buildResult(
                  dataInterval.getStart(),
                  minTime,
                  maxTime
              ).iterator();
            }

            @Override
            public void cleanup(Iterator<Result<TimeBoundaryResultValue>> toClean)
            {

            }
          }
      );
    }
  }

  /**
   * Whether a particular {@link TimeBoundaryQuery} can use {@link TimeBoundaryInspector#getMinTime()} and/or
   * {@link TimeBoundaryInspector#getMaxTime()}.
   *
   * If false, must use {@link CursorFactory#makeCursorHolder(CursorBuildSpec)}.
   */
  private static boolean canUseTimeBoundaryInspector(
      final TimeBoundaryQuery query,
      @Nullable final TimeBoundaryInspector timeBoundaryInspector
  )
  {
    if (timeBoundaryInspector == null || !timeBoundaryInspector.isMinMaxExact()) {
      return false;
    }

    if (query.getFilter() != null) {
      // We have to check which rows actually match the filter.
      return false;
    }

    final Interval queryInterval = CollectionUtils.getOnlyElement(
        query.getQuerySegmentSpec().getIntervals(),
        xs -> new IAE("Should only have one interval, got[%s]", xs)
    );

    if (!queryInterval.contains(timeBoundaryInspector.getMinMaxInterval())) {
      // Query interval does not contain segment interval. Need to create a cursor to see the first
      // timestamp within the query interval.
      return false;
    }

    // Passed all checks.
    return true;
  }

  public static CursorBuildSpec makeCursorBuildSpec(TimeBoundaryQuery query)
  {
    return CursorBuildSpec.builder()
                          .setInterval(query.getSingleInterval())
                          .setFilter(Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter())))
                          .setVirtualColumns(query.getVirtualColumns())
                          .setPhysicalColumns(query.getRequiredColumns())
                          .setQueryContext(query.context())
                          .build();
  }

  private static Pair<DateTime, DateTime> getTimeBoundary(
      final TimeBoundaryQuery query,
      final CursorFactory cursorFactory
  )
  {
    DateTime minTime = null, maxTime = null;

    if (query.needsMinTime()) {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder(makeCursorBuildSpec(query))
                         .setPreferredOrdering(Cursors.ascendingTimeOrder())
                         .build();

      try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(cursorSpec)) {
        if (cursorHolder.getTimeOrder() == Order.ASCENDING) {
          // Time-ordered cursor, use the first timestamp.
          minTime = getFirstTimestamp(query, cursorHolder);
        } else {
          // Non-time-ordered cursor. Walk and find both minTime, maxTime. Return immediately.
          return getTimeBoundaryFullScan(query, cursorHolder);
        }
      }
    }

    if (query.needsMaxTime()) {
      final CursorBuildSpec cursorSpec =
          CursorBuildSpec.builder(makeCursorBuildSpec(query))
                         .setPreferredOrdering(Cursors.descendingTimeOrder())
                         .build();

      try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(cursorSpec)) {
        if (cursorHolder.getTimeOrder() == Order.DESCENDING) {
          // Time-ordered cursor, use the first timestamp.
          maxTime = getFirstTimestamp(query, cursorHolder);
        } else {
          // Non-time-ordered cursor. Walk and find both minTime, maxTime. Return immediately.
          return getTimeBoundaryFullScan(query, cursorHolder);
        }
      }
    }

    return Pair.of(minTime, maxTime);
  }

  /**
   * Retrieve the first timestamp from {@link CursorHolder}.
   */
  @Nullable
  private static DateTime getFirstTimestamp(
      final TimeBoundaryQuery query,
      final CursorHolder cursorHolder
  )
  {
    if (query.context().getVectorize().shouldVectorize(cursorHolder.canVectorize())) {
      return getFirstTimestampVectorized(cursorHolder.asVectorCursor());
    } else {
      return getFirstTimestampNonVectorized(cursorHolder.asCursor());
    }
  }

  /**
   * Retrieve the first timestamp from {@link CursorHolder#asVectorCursor()}.
   */
  @Nullable
  private static DateTime getFirstTimestampVectorized(@Nullable final VectorCursor cursor)
  {
    if (cursor == null || cursor.isDone()) {
      return null;
    }
    final VectorValueSelector timestampSelector =
        cursor.getColumnSelectorFactory().makeValueSelector(ColumnHolder.TIME_COLUMN_NAME);
    final long[] timeVector = timestampSelector.getLongVector();
    return DateTimes.utc(timeVector[0]);
  }

  /**
   * Retrieve the first timestamp from {@link CursorHolder#asCursor()}.
   */
  @Nullable
  private static DateTime getFirstTimestampNonVectorized(@Nullable final Cursor cursor)
  {
    if (cursor == null || cursor.isDone()) {
      return null;
    }
    final BaseLongColumnValueSelector timestampSelector =
        cursor.getColumnSelectorFactory().makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
    return DateTimes.utc(timestampSelector.getLong());
  }

  /**
   * Retrieve min/max timestamps from {@link CursorHolder} using a full scan.
   */
  private static Pair<DateTime, DateTime> getTimeBoundaryFullScan(
      final TimeBoundaryQuery query,
      final CursorHolder cursorHolder
  )
  {
    if (query.context().getVectorize().shouldVectorize(cursorHolder.canVectorize())) {
      return getTimeBoundaryFullScanVectorized(query, cursorHolder.asVectorCursor());
    } else {
      return getTimeBoundaryFullScanNonVectorized(query, cursorHolder.asCursor());
    }
  }

  /**
   * Retrieve min/max timestamps from {@link CursorHolder#asVectorCursor()} using a full scan.
   */
  private static Pair<DateTime, DateTime> getTimeBoundaryFullScanVectorized(
      final TimeBoundaryQuery query,
      @Nullable final VectorCursor cursor
  )
  {
    if (cursor == null || cursor.isDone()) {
      return Pair.of(null, null);
    }

    final VectorValueSelector timeSelector =
        cursor.getColumnSelectorFactory().makeValueSelector(ColumnHolder.TIME_COLUMN_NAME);

    long minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
    while (!cursor.isDone()) {
      final long[] timeVector = timeSelector.getLongVector();
      for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
        final long timestamp = timeVector[i];
        minTime = Math.min(minTime, timestamp);
        maxTime = Math.max(maxTime, timestamp);
      }
      cursor.advance();
    }

    return Pair.of(
        query.needsMinTime() ? DateTimes.utc(minTime) : null,
        query.needsMaxTime() ? DateTimes.utc(maxTime) : null
    );
  }

  /**
   * Retrieve min/max timestamps from {@link CursorHolder#asCursor()} using a full scan.
   */
  private static Pair<DateTime, DateTime> getTimeBoundaryFullScanNonVectorized(
      final TimeBoundaryQuery query,
      @Nullable final Cursor cursor
  )
  {
    if (cursor == null || cursor.isDone()) {
      return Pair.of(null, null);
    }

    final BaseLongColumnValueSelector timeSelector =
        cursor.getColumnSelectorFactory().makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

    long minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
    while (!cursor.isDone()) {
      final long timestamp = timeSelector.getLong();
      minTime = Math.min(minTime, timestamp);
      maxTime = Math.max(maxTime, timestamp);
      cursor.advance();
    }

    return Pair.of(
        query.needsMinTime() ? DateTimes.utc(minTime) : null,
        query.needsMaxTime() ? DateTimes.utc(maxTime) : null
    );
  }
}
