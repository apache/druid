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

package org.apache.druid.segment;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.SimpleSequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A {@link StorageAdapter} that is based on a stream of objects. Generally created by a {@link RowBasedSegment}.
 *
 * @see RowBasedSegment#RowBasedSegment for implementation notes
 */
public class RowBasedStorageAdapter<RowType> implements StorageAdapter
{
  private final Sequence<RowType> rowSequence;
  private final RowAdapter<RowType> rowAdapter;
  private final RowSignature rowSignature;

  public RowBasedStorageAdapter(
      final Sequence<RowType> rowSequence,
      final RowAdapter<RowType> rowAdapter,
      final RowSignature rowSignature)
  {
    this.rowSequence = Preconditions.checkNotNull(rowSequence, "rowSequence");
    this.rowAdapter = Preconditions.checkNotNull(rowAdapter, "rowAdapter");
    this.rowSignature = Preconditions.checkNotNull(rowSignature, "rowSignature");
  }

  /**
   * Whether the provided time interval and granularity combination is allowed.
   *
   * We restrict ETERNITY with non-ALL granularity, because allowing it would involve creating a very high number
   * of time grains. This would cause queries to take an excessive amount of time or run out of memory.
   */
  public static boolean isQueryGranularityAllowed(final Interval interval, final Granularity granularity)
  {
    return Granularities.ALL.equals(granularity) || !Intervals.ETERNITY.equals(interval);
  }

  @Override
  public Interval getInterval()
  {
    return Intervals.ETERNITY;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<>(new ArrayList<>(rowSignature.getColumnNames()));
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return Collections.emptyList();
  }

  @Override
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public DateTime getMinTime()
  {
    return getInterval().getStart();
  }

  @Override
  public DateTime getMaxTime()
  {
    return getInterval().getEnd().minus(1);
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    return null;
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    return null;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return RowBasedColumnSelectorFactory.getColumnCapabilities(rowSignature, column);
  }

  @Override
  public int getNumRows()
  {
    if (rowSequence instanceof SimpleSequence) {
      final Iterable<RowType> rowIterable = ((SimpleSequence<RowType>) rowSequence).getIterable();

      if (rowIterable instanceof Collection) {
        return ((Collection<RowType>) rowIterable).size();
      }
    }

    // getNumRows is only used by tests and by segmentMetadataQuery (which would be odd to call on inline datasources)
    // so no big deal if it doesn't always work.
    throw new UnsupportedOperationException("Cannot retrieve number of rows");
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return getMaxTime();
  }

  @Override
  public Metadata getMetadata()
  {
    throw new UnsupportedOperationException("Cannot retrieve metadata");
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable final Filter filter,
      final Interval queryInterval,
      final VirtualColumns virtualColumns,
      final Granularity gran,
      final boolean descending,
      @Nullable final QueryMetrics<?> queryMetrics
  )
  {
    final Interval actualInterval = queryInterval.overlap(new Interval(getMinTime(), gran.bucketEnd(getMaxTime())));

    if (actualInterval == null) {
      return Sequences.empty();
    }

    if (!isQueryGranularityAllowed(actualInterval, gran)) {
      throw new IAE(
          "Cannot support interval [%s] with granularity [%s]",
          Intervals.ETERNITY.equals(actualInterval) ? "ETERNITY" : actualInterval,
          gran
      );
    }

    final RowWalker<RowType> rowWalker = new RowWalker<>(
        descending ? reverse(rowSequence) : rowSequence,
        rowAdapter
    );

    final Iterable<Interval> bucketIntervals = gran.getIterable(actualInterval);

    return Sequences.simple(
        Iterables.transform(
            descending ? reverse(bucketIntervals) : bucketIntervals,
            bucketInterval ->
                (Cursor) new RowBasedCursor<>(
                    rowWalker,
                    rowAdapter,
                    filter,
                    bucketInterval,
                    virtualColumns,
                    gran,
                    descending,
                    rowSignature
                )
        )
    ).withBaggage(rowWalker::close);
  }

  /**
   * Reverse a Sequence.
   *
   * If the Sequence is a {@link SimpleSequence}, this avoids materialization because its
   * {@link SimpleSequence#toList()} method returns a view of the underlying list. Otherwise, the list will be
   * materialized and then reversed.
   */
  private static <T> Sequence<T> reverse(final Sequence<T> sequence)
  {
    if (sequence instanceof SimpleSequence) {
      // Extract the Iterable from the SimpleSequence, so we can reverse it without copying if it is List-backed.
      return Sequences.simple(reverse(((SimpleSequence<T>) sequence).getIterable()));
    } else {
      // Materialize and reverse the objects.
      return Sequences.simple(Lists.reverse(sequence.toList()));
    }
  }

  /**
   * Reverse an Iterable. Will avoid materialization if possible, but, this is not always possible.
   */
  private static <T> Iterable<T> reverse(final Iterable<T> iterable)
  {
    if (iterable instanceof List) {
      return Lists.reverse((List<T>) iterable);
    } else {
      // Materialize and reverse the objects. Note that this means reversing non-List Iterables will use extra memory.
      return Lists.reverse(Lists.newArrayList(iterable));
    }
  }
}
