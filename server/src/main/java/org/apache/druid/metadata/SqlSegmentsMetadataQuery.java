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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An object that helps {@link SqlSegmentsMetadataManager} and {@link IndexerSQLMetadataStorageCoordinator} make
 * queries to the metadata store segments table. Each instance of this class is scoped to a single handle and is meant
 * to be short-lived.
 */
public class SqlSegmentsMetadataQuery
{
  private static final Logger log = new Logger(SqlSegmentsMetadataQuery.class);

  private final Handle handle;
  private final SQLMetadataConnector connector;
  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;

  private SqlSegmentsMetadataQuery(
      final Handle handle,
      final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig dbTables,
      final ObjectMapper jsonMapper
  )
  {
    this.handle = handle;
    this.connector = connector;
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
  }

  /**
   * Create a query object. This instance is scoped to a single handle and is meant to be short-lived. It is okay
   * to use it for more than one query, though.
   */
  public static SqlSegmentsMetadataQuery forHandle(
      final Handle handle,
      final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig dbTables,
      final ObjectMapper jsonMapper
  )
  {
    return new SqlSegmentsMetadataQuery(handle, connector, dbTables, jsonMapper);
  }

  /**
   * Retrieves segments for a given datasource that are marked used (i.e. published) in the metadata store, and that
   * *overlap* any interval in a particular collection of intervals. If the collection of intervals is empty, this
   * method will retrieve all used segments.
   *
   * You cannot assume that segments returned by this call are actually active. Because there is some delay between
   * new segment publishing and the marking-unused of older segments, it is possible that some segments returned
   * by this call are overshadowed by other segments. To check for this, use
   * {@link org.apache.druid.timeline.VersionedIntervalTimeline#forSegments(Iterator)}.
   *
   * This call does not return any information about realtime segments.
   *
   * Returns a closeable iterator. You should close it when you are done.
   */
  public CloseableIterator<DataSegment> retrieveUsedSegments(
      final String dataSource,
      final Collection<Interval> intervals
  )
  {
    return retrieveSegments(dataSource, intervals, IntervalMode.OVERLAPS, true);
  }

  /**
   * Retrieves segments for a given datasource that are marked unused and that are *fully contained by* any interval
   * in a particular collection of intervals. If the collection of intervals is empty, this method will retrieve all
   * unused segments.
   *
   * This call does not return any information about realtime segments.
   *
   * Returns a closeable iterator. You should close it when you are done.
   */
  public CloseableIterator<DataSegment> retrieveUnusedSegments(
      final String dataSource,
      final Collection<Interval> intervals
  )
  {
    return retrieveSegments(dataSource, intervals, IntervalMode.CONTAINS, false);
  }

  /**
   * Marks the provided segments as either used or unused.
   *
   * Returns the number of segments actually modified.
   */
  public int markSegments(final Collection<SegmentId> segmentIds, final boolean used)
  {
    final String dataSource;

    if (segmentIds.isEmpty()) {
      return 0;
    } else {
      dataSource = segmentIds.iterator().next().getDataSource();
      if (segmentIds.stream().anyMatch(segment -> !dataSource.equals(segment.getDataSource()))) {
        throw new IAE("Segments to drop must all be part of the same datasource");
      }
    }

    final PreparedBatch batch =
        handle.prepareBatch(
            StringUtils.format(
                "UPDATE %s SET used = ? WHERE datasource = ? AND id = ?",
                dbTables.getSegmentsTable()
            )
        );

    for (SegmentId segmentId : segmentIds) {
      batch.add(used, dataSource, segmentId.toString());
    }

    final int[] segmentChanges = batch.execute();
    return computeNumChangedSegments(
        segmentIds.stream().map(SegmentId::toString).collect(Collectors.toList()),
        segmentChanges
    );
  }

  /**
   * Marks all segments for a datasource unused that are *fully contained by* a particular interval.
   *
   * Returns the number of segments actually modified.
   */
  public int markSegmentsUnused(final String dataSource, final Interval interval)
  {
    if (Intervals.isEternity(interval)) {
      return handle
          .createStatement(
              StringUtils.format(
                  "UPDATE %s SET used=:used WHERE dataSource = :dataSource",
                  dbTables.getSegmentsTable()
              )
          )
          .bind("dataSource", dataSource)
          .bind("used", false)
          .execute();
    } else if (Intervals.canCompareEndpointsAsStrings(interval)
               && interval.getStart().getYear() == interval.getEnd().getYear()) {
      // Safe to write a WHERE clause with this interval. Note that it is unsafe if the years are different, because
      // that means extra characters can sneak in. (Consider a query interval like "2000-01-01/2001-01-01" and a
      // segment interval like "20001/20002".)
      return handle
          .createStatement(
              StringUtils.format(
                  "UPDATE %s SET used=:used WHERE dataSource = :dataSource AND %s",
                  dbTables.getSegmentsTable(),
                  IntervalMode.CONTAINS.makeSqlCondition(connector.getQuoteString(), ":start", ":end")
              )
          )
          .bind("dataSource", dataSource)
          .bind("used", false)
          .bind("start", interval.getStart().toString())
          .bind("end", interval.getEnd().toString())
          .execute();
    } else {
      // Retrieve, then drop, since we can't write a WHERE clause directly.
      final List<SegmentId> segments = ImmutableList.copyOf(
          Iterators.transform(
              retrieveSegments(dataSource, Collections.singletonList(interval), IntervalMode.CONTAINS, true),
              DataSegment::getId
          )
      );
      return markSegments(segments, false);
    }
  }

  private CloseableIterator<DataSegment> retrieveSegments(
      final String dataSource,
      final Collection<Interval> intervals,
      final IntervalMode matchMode,
      final boolean used
  )
  {
    // Check if the intervals all support comparing as strings. If so, bake them into the SQL.
    final boolean compareAsString = intervals.stream().allMatch(Intervals::canCompareEndpointsAsStrings);

    final StringBuilder sb = new StringBuilder();
    sb.append("SELECT payload FROM %s WHERE used = :used AND dataSource = :dataSource");

    if (compareAsString && !intervals.isEmpty()) {
      sb.append(" AND (");
      for (int i = 0; i < intervals.size(); i++) {
        sb.append(
            matchMode.makeSqlCondition(
                connector.getQuoteString(),
                StringUtils.format(":start%d", i),
                StringUtils.format(":end%d", i)
            )
        );

        // Add a special check for a segment which have one end at eternity and the other at some finite value. Since
        // we are using string comparison, a segment with this start or end will not be returned otherwise.
        if (matchMode.equals(IntervalMode.OVERLAPS)) {
          sb.append(StringUtils.format(" OR (start = '%s' AND \"end\" != '%s' AND \"end\" > :start%d)", Intervals.ETERNITY.getStart(), Intervals.ETERNITY.getEnd(), i));
          sb.append(StringUtils.format(" OR (start != '%s' AND \"end\" = '%s' AND start < :end%d)", Intervals.ETERNITY.getStart(), Intervals.ETERNITY.getEnd(), i));
        }

        if (i != intervals.size() - 1) {
          sb.append(" OR ");
        }
      }

      // Add a special check for a single segment with eternity. Since we are using string comparison, a segment with
      // this start and end will not be returned otherwise.
      // Known Issue: https://github.com/apache/druid/issues/12860
      if (matchMode.equals(IntervalMode.OVERLAPS)) {
        sb.append(StringUtils.format(" OR (start = '%s' AND \"end\" = '%s')", Intervals.ETERNITY.getStart(), Intervals.ETERNITY.getEnd()));
      }
      sb.append(")");
    }

    final Query<Map<String, Object>> sql = handle
        .createQuery(StringUtils.format(sb.toString(), dbTables.getSegmentsTable()))
        .setFetchSize(connector.getStreamingFetchSize())
        .bind("used", used)
        .bind("dataSource", dataSource);

    if (compareAsString) {
      final Iterator<Interval> iterator = intervals.iterator();
      for (int i = 0; iterator.hasNext(); i++) {
        Interval interval = iterator.next();
        sql.bind(StringUtils.format("start%d", i), interval.getStart().toString())
           .bind(StringUtils.format("end%d", i), interval.getEnd().toString());
      }
    }

    final ResultIterator<DataSegment> resultIterator =
        sql.map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class))
           .iterator();

    return CloseableIterators.wrap(
        Iterators.filter(
            resultIterator,
            dataSegment -> {
              if (intervals.isEmpty()) {
                return true;
              } else {
                // Must re-check that the interval matches, even if comparing as string, because the *segment interval*
                // might not be string-comparable. (Consider a query interval like "2000-01-01/3000-01-01" and a
                // segment interval like "20010/20011".)
                for (Interval interval : intervals) {
                  if (matchMode.apply(interval, dataSegment.getInterval())) {
                    return true;
                  }
                }

                return false;
              }
            }
        ),
        resultIterator
    );
  }

  private static int computeNumChangedSegments(List<String> segmentIds, int[] segmentChanges)
  {
    int numChangedSegments = 0;
    for (int i = 0; i < segmentChanges.length; i++) {
      int numUpdatedRows = segmentChanges[i];
      if (numUpdatedRows < 0) {
        log.assertionError(
            "Negative number of rows updated for segment id [%s]: %d",
            segmentIds.get(i),
            numUpdatedRows
        );
      } else if (numUpdatedRows > 1) {
        log.error(
            "More than one row updated for segment id [%s]: %d, "
            + "there may be more than one row for the segment id in the database",
            segmentIds.get(i),
            numUpdatedRows
        );
      }
      if (numUpdatedRows > 0) {
        numChangedSegments += 1;
      }
    }
    return numChangedSegments;
  }

  enum IntervalMode
  {
    CONTAINS {
      @Override
      public String makeSqlCondition(String quoteString, String startPlaceholder, String endPlaceholder)
      {
        // 2 range conditions are used on different columns, but not all SQL databases properly optimize it.
        // Some databases can only use an index on one of the columns. An additional condition provides
        // explicit knowledge that 'start' cannot be greater than 'end'.
        return StringUtils.format(
            "(start >= %2$s and start <= %3$s and %1$send%1$s <= %3$s)",
            quoteString,
            startPlaceholder,
            endPlaceholder
        );
      }

      @Override
      public boolean apply(Interval a, Interval b)
      {
        return a.contains(b);
      }
    },
    OVERLAPS {
      @Override
      public String makeSqlCondition(String quoteString, String startPlaceholder, String endPlaceholder)
      {
        return StringUtils.format(
            "(start < %3$s AND %1$send%1$s > %2$s)",
            quoteString,
            startPlaceholder,
            endPlaceholder
        );
      }

      @Override
      public boolean apply(Interval a, Interval b)
      {
        return a.overlaps(b);
      }
    };

    public abstract String makeSqlCondition(String quoteString, String startPlaceholder, String endPlaceholder);

    public abstract boolean apply(Interval a, Interval b);
  }
}
