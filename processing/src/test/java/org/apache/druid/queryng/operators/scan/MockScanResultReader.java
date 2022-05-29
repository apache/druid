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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.ResultIterator;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockScanResultReader implements IterableOperator<ScanResultValue>
{
  public String segmentId = "mock-segment";
  protected final List<String> columns;
  public ResultFormat resultFormat;
  private final int targetCount;
  private final int batchSize;
  private final long msPerRow;
  private long nextTs;
  private int rowCount;
  @SuppressWarnings("unused")
  private int batchCount;

  /**
   * State allows tests to verify that the operator protocol
   * was followed. Not really necessary here functionally, so this
   * is just a test tool.
   */
  public State state = State.START;

  public MockScanResultReader(
      FragmentContext context,
      int columnCount,
      int targetCount,
      int batchSize,
      Interval interval)
  {
    this(
        context,
        columnCount,
        targetCount,
        batchSize,
        interval,
        ResultFormat.RESULT_FORMAT_COMPACTED_LIST);
  }

  public MockScanResultReader(
      FragmentContext context,
      int columnCount,
      int targetCount,
      int batchSize,
      Interval interval,
      ResultFormat resultFormat)
  {
    this.columns = new ArrayList<>(columnCount);
    if (columnCount > 0) {
      columns.add(ColumnHolder.TIME_COLUMN_NAME);
    }
    for (int i = 1; i < columnCount; i++) {
      columns.add("Column" + i);
    }
    this.targetCount = targetCount;
    this.batchSize = batchSize;
    this.resultFormat = resultFormat;
    if (targetCount == 0) {
      this.msPerRow = 0;
    } else {
      this.msPerRow = Math.toIntExact(interval.toDurationMillis() / targetCount);
    }
    this.nextTs = interval.getStartMillis();
    context.register(this);
  }

  public static Interval interval(int offset)
  {
    Duration grain = Duration.ofMinutes(1);
    Instant base = Instant.parse("2021-10-24T00:00:00Z");
    Duration grainOffset = grain.multipliedBy(offset);
    Instant start = base.plus(grainOffset);
    return Intervals.utc(start.toEpochMilli(), start.plus(grain).toEpochMilli());
  }

  @Override
  public ResultIterator<ScanResultValue> open()
  {
    state = State.RUN;
    return this;
  }

  @Override
  public ScanResultValue next() throws ResultIterator.EofException
  {
    Preconditions.checkState(state == State.RUN);
    if (rowCount >= targetCount) {
      throw Operators.eof();
    }
    int n = Math.min(targetCount - rowCount, batchSize);
    Object batch;
    if (resultFormat == ResultFormat.RESULT_FORMAT_COMPACTED_LIST) {
      batch = compactBatch(n);
    } else {
      batch = listBatch(n);
    }
    batchCount++;
    return new ScanResultValue(segmentId, columns, batch);
  }

  private Object compactBatch(int n)
  {
    List<List<Object>> batch = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      List<Object> values = new ArrayList<>(columns.size());
      if (!columns.isEmpty()) {
        values.add(nextTs);
      }
      for (int j = 1; j < columns.size(); j++) {
        values.add(StringUtils.format("Value %d.%d", rowCount, j));
      }
      batch.add(values);
      rowCount++;
      nextTs += msPerRow;
    }
    return batch;
  }

  private Object listBatch(int n)
  {
    List<Map<String, Object>> batch = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      Map<String, Object> values = Maps.newHashMapWithExpectedSize(columns.size());
      if (!columns.isEmpty()) {
        values.put(ColumnHolder.TIME_COLUMN_NAME, nextTs);
      }
      for (int j = 1; j < columns.size(); j++) {
        values.put(columns.get(j), StringUtils.format("Value %d.%d", rowCount, j));
      }
      batch.add(values);
      rowCount++;
      nextTs += msPerRow;
    }
    return batch;
  }

  @Override
  public void close(boolean cascade)
  {
    state = State.CLOSED;
  }

  @VisibleForTesting
  public static long getTime(List<Object> row)
  {
    return (Long) row.get(0);
  }

  @VisibleForTesting
  public static long getFirstTime(Object row)
  {
    ScanResultValue value = (ScanResultValue) row;
    List<List<Object>> events = value.getRows();
    if (events.isEmpty()) {
      return 0;
    }
    return getTime(events.get(0));
  }

  @VisibleForTesting
  public static long getLastTime(Object row)
  {
    ScanResultValue value = (ScanResultValue) row;
    List<List<Object>> events = value.getRows();
    if (events.isEmpty()) {
      return 0;
    }
    return getTime(events.get(events.size() - 1));
  }
}
