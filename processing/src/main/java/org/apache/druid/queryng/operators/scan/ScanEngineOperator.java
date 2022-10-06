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

package org.apache.druid.queryng.operators.scan;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Iterators;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.ResultIterator;
import org.apache.druid.queryng.operators.SequenceIterator;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Implements a scan query against a fragment using a storage adapter which may
 * return one or more cursors for the segment. Each cursor is processed using
 * a {@link CursorReader}. The set of cursors is known only at run time.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryEngine}
 */
public class ScanEngineOperator implements Operator<ScanResultValue>
{
  public static final String LEGACY_TIMESTAMP_KEY = "timestamp";

  public enum Order
  {
    NONE,
    ASCENDING,
    DESCENDING
  }

  // TODO: Unify this with the class of the same name for time series.
  public static class CursorDefinition
  {
    private final Segment segment;
    private final Interval queryInterval;
    private final Filter filter;
    private final VirtualColumns virtualColumns;
    private final boolean descending;
    private final Granularity granularity;
    @Nullable private final QueryMetrics<?> queryMetrics;
    private StorageAdapter adapter;

    public CursorDefinition(
        final Segment segment,
        final Interval queryInterval,
        @Nullable final Filter filter,
        @Nullable final VirtualColumns virtualColumns,
        final boolean descending,
        final Granularity granularity,
        @Nullable final QueryMetrics<?> queryMetrics
    )
    {
      this.segment = segment;
      this.queryInterval = queryInterval;
      this.filter = filter;
      this.virtualColumns = virtualColumns;
      this.descending = descending;
      this.granularity = granularity;
      this.queryMetrics = queryMetrics;
    }

    private StorageAdapter adapter()
    {
      if (adapter == null) {
        adapter = segment.asStorageAdapter();
        if (adapter == null) {
          throw new ISE(
              "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
          );
        }
      }
      return adapter;
    }

    public SequenceIterator<Cursor> cursors()
    {
      return SequenceIterator.of(
          adapter().makeCursors(
              filter,
              queryInterval,
              virtualColumns,
              granularity,
              descending,
              queryMetrics
          )
      );
    }
  }

  /**
   * Inner class which holds the state for reading a cursor. Allows
   * some read state to be final.
   */
  private class ScanIterator implements ResultIterator<ScanResultValue>
  {
    private final SequenceIterator<Cursor> iter;
    private final List<String> selectedColumns;
    private final long limit;
    private CursorReader cursorReader;

    private ScanIterator(long limit)
    {
      this.limit = limit;
      iter = cursorDefn.cursors();
      if (columns == null) {
        selectedColumns = inferColumns();
      } else {
        selectedColumns = columns;
      }
    }

    protected List<String> inferColumns()
    {
      final Set<String> availableColumns = Sets.newLinkedHashSet(
          Iterables.concat(
              Collections.singleton(isLegacy ? LEGACY_TIMESTAMP_KEY : ColumnHolder.TIME_COLUMN_NAME),
              Iterables.transform(
                  Arrays.asList(cursorDefn.virtualColumns.getVirtualColumns()),
                  VirtualColumn::getOutputName
              ),
              cursorDefn.adapter.getAvailableDimensions(),
              cursorDefn.adapter.getAvailableMetrics()
          )
      );

      if (isLegacy) {
        availableColumns.remove(ColumnHolder.TIME_COLUMN_NAME);
      }
      return new ArrayList<>(availableColumns);
    }

    /**
     * Return the next batch of events from a cursor. Enforce the timeout limit.
     * Another batch of events is available if
     * we have (or can get) a cursor which has rows, and we are not at the
     * limit set for this operator.
     * @throws ResultIterator.EofException
     */
    @Override
    public ScanResultValue next() throws ResultIterator.EofException
    {
      while (true) {
        context.checkTimeout();
        if (cursorReader != null) {
          try {
            // Happy path
            List<?> result = (List<?>) cursorReader.next();
            batchCount++;
            rowCount += result.size();
            return new ScanResultValue(
                cursorDefn.segment.getId().toString(),
                selectedColumns,
                result
            );
          }
          catch (ResultIterator.EofException e) {
            // Cursor is done or was empty.
            closeCursorReader();
          }
        }
        if (iter == null) {
          // Done previously
          throw Operators.eof();
        }
        if (rowCount > limit) {
          // Reached row limit
          finish();
          throw Operators.eof();
        }
        if (!iter.hasNext()) {
          // No more cursors
          finish();
          throw Operators.eof();
        }
        // Read from the next cursor.
        cursorReader = new CursorReader(
            iter.next(),
            selectedColumns,
            limit - rowCount,
            batchSize,
            resultFormat,
            isLegacy,
            timeoutAt,
            context.queryId()
        );
        cursorCount++;
      }
    }

    private void closeCursorReader()
    {
      if (cursorReader != null) {
        cursorReader = null;
      }
    }

    private void finish()
    {
      closeCursorReader();
      context.responseContext().addRowScanCount(rowCount);
      iter.close();
    }
  }

  protected final FragmentContext context;
  private final CursorDefinition cursorDefn;
  private final List<String> columns;
  private final boolean isLegacy;
  private final int batchSize;
  private final Order order;
  private final long scanLimit;
  private final ResultFormat resultFormat;
  private final long timeoutAt;
  private ScanIterator impl;
  private int rowCount;
  private int batchCount;
  private int cursorCount;

  public ScanEngineOperator(
      final FragmentContext context,
      final CursorDefinition cursorDefn,
      final int batchSize,
      final boolean isLegacy,
      final List<String> columns,
      final Order order,
      final long scanLimit,
      final ResultFormat resultFormat,
      final long timeoutAt
  )
  {
    this.context = context;
    this.cursorDefn = cursorDefn;
    this.isLegacy = isLegacy;
    this.batchSize = batchSize;
    this.columns = columns;
    this.order = order;
    this.scanLimit = scanLimit;
    this.resultFormat = resultFormat;
    this.timeoutAt = timeoutAt;
    context.register(this);
  }

  @Override
  public ResultIterator<ScanResultValue> open()
  {
    ResponseContext responseContext = context.responseContext();
    // If the row count is not set, set it to 0, else do nothing.
    responseContext.addRowScanCount(0);
    final long limit;
    if (order == Order.NONE) {
      limit = scanLimit - responseContext.getRowScanCount();
    } else {
      // If we're performing time-ordering, we want to scan through the first
      // `limit` rows in each segment ignoring the number of rows already
      // counted on other segments.
      limit = scanLimit;
    }

    if (limit <= 0) {
      return Iterators.emptyIterator();
    } else {
      impl = new ScanIterator(limit);
      return impl;
    }
  }

  @Override
  public void close(boolean cascade)
  {
    if (impl != null) {
      impl.closeCursorReader();
      OperatorProfile profile = new OperatorProfile("scan-query");
      profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
      profile.add(OperatorProfile.BATCH_COUNT_METRIC, batchCount);
      profile.add("cursorCount", cursorCount);
      context.updateProfile(this, profile);
    }
    impl = null;
  }
}
