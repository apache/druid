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

package org.apache.druid.queryng.operators.general;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Collections;

public class MockStorageAdapter implements StorageAdapter
{
  public static final Interval MOCK_INTERVAL = Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T14:00:00.000Z");
  public static final SegmentDescriptor MOCK_DESCRIPTOR = new SegmentDescriptor(
      MOCK_INTERVAL,
      "1",
      1);

  public static class MockSegment implements Segment
  {
    protected final int segmentSize;

    public MockSegment(int segmentSize)
    {
      this.segmentSize = segmentSize;
    }

    @Override
    public void close()
    {
    }

    @Override
    public SegmentId getId()
    {
      return SegmentId.of(
          "dummyDs",
          MOCK_DESCRIPTOR.getInterval(),
          MOCK_DESCRIPTOR.getVersion(),
          MOCK_DESCRIPTOR.getPartitionNumber());
    }

    @Override
    public Interval getDataInterval()
    {
      return MockStorageAdapter.MOCK_INTERVAL;
    }

    @Override
    public QueryableIndex asQueryableIndex()
    {
      return null;
    }

    @Override
    public StorageAdapter asStorageAdapter()
    {
      if (segmentSize < 0) {
        // Simulate no segment available
        return null;
      }
      return new MockStorageAdapter(segmentSize);
    }
  }

  private final int segmentSize;

  public MockStorageAdapter()
  {
    this.segmentSize = 5_000_000;
  }

  public MockStorageAdapter(int segmentSize)
  {
    this.segmentSize = segmentSize;
  }

  @Override
  public Sequence<Cursor> makeCursors(
      Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      QueryMetrics<?> queryMetrics)
  {
    return Sequences.simple(Collections.singletonList(new MockCursor(interval, segmentSize)));
  }

  @Override
  public Interval getInterval()
  {
    return MOCK_INTERVAL;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<>(Arrays.asList(ColumnHolder.TIME_COLUMN_NAME, "page"));
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return Collections.singletonList("delta");
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    return segmentSize;
  }

  @Override
  public DateTime getMinTime()
  {
    return DateTimes.of("2015-09-12T13:00:00.000Z");
  }

  @Override
  public DateTime getMaxTime()
  {
    return DateTimes.of("2015-09-12T13:59:59.999Z");
  }

  @Override
  public Comparable<?> getMinValue(String column)
  {
    switch (column) {
      case "__time":
        return getMinTime();
      case "delta":
        return 0;
      case "page":
        return "";
    }
    return null;
  }

  @Override
  public Comparable<?> getMaxValue(String column)
  {
    switch (column) {
      case "__time":
        return getMaxTime();
      case "delta":
        return 10_000;
      case "page":
        return "zzzzzzzzzz";
    }
    return null;
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return null;
  }

  @Override
  public int getNumRows()
  {
    return segmentSize;
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return getMaxTime();
  }

  @Override
  public Metadata getMetadata()
  {
    return null;
  }
}
