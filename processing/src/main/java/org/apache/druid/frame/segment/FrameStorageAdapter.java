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

package org.apache.druid.frame.segment;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;

/**
 * A {@link StorageAdapter} implementation based on a single {@link Frame}.
 *
 * This class is used for both columnar and row-based frames.
 */
public class FrameStorageAdapter implements StorageAdapter
{
  private final Frame frame;
  private final FrameReader frameReader;
  private final Interval interval;
  private final CursorFactory cursorFactory;

  public FrameStorageAdapter(Frame frame, FrameReader frameReader, Interval interval)
  {
    this.frame = frame;
    this.frameReader = frameReader;
    this.interval = interval;
    this.cursorFactory = frameReader.makeCursorFactory(frame);
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return frameReader.signature();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<>(frameReader.signature().getColumnNames());
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return Collections.emptyList();
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
    // It's ok to return null always, because callers are required to handle the case where the min value is not known.
    return null;
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    // It's ok to return null always, because callers are required to handle the case where the max value is not known.
    return null;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return frameReader.columnCapabilities(frame, column);
  }

  @Override
  public int getNumRows()
  {
    return frame.numRows();
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return getMaxTime();
  }

  @Override
  @Nullable
  public Metadata getMetadata()
  {
    return null;
  }

  @Override
  public boolean canVectorize(@Nullable Filter filter, VirtualColumns virtualColumns, boolean descending)
  {
    return cursorFactory.canVectorize(filter, virtualColumns, descending);
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    return cursorFactory.makeCursors(
        filter,
        interval,
        virtualColumns,
        gran,
        descending,
        queryMetrics
    );
  }

  @Nullable
  @Override
  public VectorCursor makeVectorCursor(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      boolean descending,
      int vectorSize,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    return cursorFactory.makeVectorCursor(
        filter,
        interval,
        virtualColumns,
        descending,
        vectorSize,
        queryMetrics
    );
  }
}
