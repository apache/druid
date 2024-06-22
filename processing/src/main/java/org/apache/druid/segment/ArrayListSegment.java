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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.rowsandcols.ArrayListRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;

/**
 * A {@link Segment} that is based on a stream of objects.
 */
public class ArrayListSegment<RowType> implements Segment
{
  private final SegmentId segmentId;
  private final ArrayList<RowType> rows;
  private final RowAdapter<RowType> rowAdapter;
  private final RowSignature rowSignature;

  /**
   * Create a list-based segment.
   * <p>
   * The provided List must be in time-order according to the provided {@link RowAdapter#timestampFunction()}.
   * The cursor returned by {@link RowBasedStorageAdapter#makeCursors} makes no attempt to verify this, and callers
   * will expect it.
   * <p>
   * The provided "rowSignature" will be used for reporting available columns and their capabilities to users of
   * {@link #asStorageAdapter()}. Note that the {@link ColumnSelectorFactory} implementation returned by this segment's
   * storage adapter will allow creation of selectors on any field, using the {@link RowAdapter#columnFunction} for that
   * field, even if it doesn't appear in "rowSignature".
   *
   * @param segmentId    segment identifier; will be returned by {@link #getId()}
   * @param rows         objects that comprise this segment. Must be re-iterable if support for {@link Cursor#reset()}
   *                     is required. Otherwise, does not need to be re-iterable.
   * @param rowAdapter   adapter used for reading these objects
   * @param rowSignature signature of the columns in these objects
   */
  public ArrayListSegment(
      final SegmentId segmentId,
      final ArrayList<RowType> rows,
      final RowAdapter<RowType> rowAdapter,
      final RowSignature rowSignature
  )
  {
    this.segmentId = Preconditions.checkNotNull(segmentId, "segmentId");
    this.rows = rows;
    this.rowAdapter = rowAdapter;
    this.rowSignature = rowSignature;
  }

  @Override
  @Nonnull
  public SegmentId getId()
  {
    return segmentId;
  }

  @Override
  @Nonnull
  public Interval getDataInterval()
  {
    return Intervals.ETERNITY;
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Override
  @Nonnull
  public StorageAdapter asStorageAdapter()
  {
    return new RowBasedStorageAdapter<>(Sequences.simple(rows), rowAdapter, rowSignature);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz)
  {
    if (CloseableShapeshifter.class.equals(clazz)) {
      return (T) new MyCloseableShapeshifter();
    }
    return Segment.super.as(clazz);
  }

  @Override
  public void close()
  {
    // Do nothing.
  }

  private RowsAndColumns asRowsAndColumns()
  {
    return new ArrayListRowsAndColumns<>(rows, rowAdapter, rowSignature);
  }

  private class MyCloseableShapeshifter implements CloseableShapeshifter
  {

    @Override
    public void close()
    {

    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public <T> T as(Class<T> clazz)
    {
      if (RowsAndColumns.class.equals(clazz)) {
        return (T) asRowsAndColumns();
      }
      return null;
    }
  }
}
