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

package org.apache.druid.query.vector;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.queryng.operators.general.MockStorageAdapter;
import org.apache.druid.queryng.operators.timeseries.TimeGroupVectorIterator;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VectorCursorGranularizerTest
{
  public static final int BATCH_SIZE = 3;

  private static class MockLongVectorSelector extends BaseLongVectorValueSelector
  {
    private long[] dataSet;
    private long[] vector;

    public MockLongVectorSelector(ReadableVectorOffset offset, long[] dataSet)
    {
      super(offset);
      this.dataSet = dataSet;
      setVector();
    }

    public void setVector()
    {
      if (offset.getStartOffset() >= dataSet.length) {
        this.vector = null;
      } else {
        int from = offset.getStartOffset();
        int to = from + getCurrentVectorSize();
        this.vector = Arrays.copyOfRange(dataSet, from, to);
      }
    }

    @Override
    public long[] getLongVector()
    {
      return vector;
    }

    @Override
    public boolean[] getNullVector()
    {
      return null;
    }
  }

  private static class MockVectorColumnSelectorFactory implements VectorColumnSelectorFactory
  {
    private final MockLongVectorSelector timeSelector;

    private MockVectorColumnSelectorFactory(MockLongVectorSelector timeSelector)
    {
      this.timeSelector = timeSelector;
    }

    @Override
    public ReadableVectorInspector getReadableVectorInspector()
    {
      return null;
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public VectorValueSelector makeValueSelector(String column)
    {
      return timeSelector;
    }

    @Override
    public VectorObjectSelector makeObjectSelector(String column)
    {
      return null;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  }

  private static class MockVectorCursor implements VectorCursor
  {
    private final VectorOffset offset;
    private final MockLongVectorSelector timeSelector;
    private final MockVectorColumnSelectorFactory selectorFactory;

    public MockVectorCursor(long[] times, int batchSize)
    {
      this.offset = new NoFilterVectorOffset(batchSize, 0, times.length);
      this.timeSelector = new MockLongVectorSelector(offset, times);
      this.selectorFactory = new MockVectorColumnSelectorFactory(timeSelector);
    }

    public MockVectorCursor(int n)
    {
      this.offset = new NoFilterVectorOffset(BATCH_SIZE, 0, n);
      this.timeSelector = null;
      this.selectorFactory = null;
    }

    @Override
    public int getMaxVectorSize()
    {
      return offset.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return offset.getCurrentVectorSize();
    }

    @Override
    public VectorColumnSelectorFactory getColumnSelectorFactory()
    {
      return selectorFactory;
    }

    @Override
    public void advance()
    {
      offset.advance();
      if (timeSelector != null) {
        timeSelector.setVector();
      }
    }

    @Override
    public boolean isDone()
    {
      return offset.isDone();
    }

    @Override
    public void reset()
    {
      offset.reset();
    }

    @Override
    public void close()
    {
    }
  }

  // Test the vector offset

  /**
   * Offset on an empty segment: either zero-length or zero-length window.
   */
  @Test
  public void testNoFilterVectorOffset_Empty()
  {
    VectorOffset offset = new NoFilterVectorOffset(10, 0, 0);
    assertTrue(offset.isContiguous());
    assertEquals(10, offset.getMaxVectorSize());
    assertTrue(offset.isDone());

    offset.reset();
    assertTrue(offset.isDone());

    offset = new NoFilterVectorOffset(10, 10, 10);
    assertTrue(offset.isContiguous());
    assertEquals(10, offset.getMaxVectorSize());
    assertTrue(offset.isDone());

    offset.reset();
    assertTrue(offset.isDone());
  }

  /**
   * Vector offset over segment with a single batch smaller than the vector size.
   */
  @Test
  public void testNoFilterVectorOffset_OneBatch()
  {
    VectorOffset offset = new NoFilterVectorOffset(10, 0, 1);
    assertTrue(offset.isContiguous());
    assertEquals(10, offset.getMaxVectorSize());

    assertFalse(offset.isDone());
    assertEquals(0, offset.getStartOffset());
    assertEquals(1, offset.getCurrentVectorSize());

    offset.advance();
    assertTrue(offset.isDone());

    offset.reset();
    assertFalse(offset.isDone());
    offset.advance();
    assertTrue(offset.isDone());
  }

  /**
   * Vector offset over a segment with a single batch of the same size as the vector.
   */
  @Test
  public void testNoFilterVectorOffset_OneFullBatch()
  {
    VectorOffset offset = new NoFilterVectorOffset(10, 0, 10);
    assertTrue(offset.isContiguous());
    assertEquals(0, offset.getStartOffset());
    assertEquals(10, offset.getMaxVectorSize());
    assertEquals(10, offset.getCurrentVectorSize());
    assertFalse(offset.isDone());

    offset.advance();
    assertTrue(offset.isDone());

    offset.reset();
    assertFalse(offset.isDone());
    offset.advance();
    assertTrue(offset.isDone());
  }

  /**
   * Vector offset on a segment that requires two vectors.
   */
  @Test
  public void testNoFilterVectorOffset_TwoBatches()
  {
    VectorOffset offset = new NoFilterVectorOffset(10, 0, 15);
    assertTrue(offset.isContiguous());
    assertEquals(10, offset.getMaxVectorSize());

    assertFalse(offset.isDone());
    assertEquals(0, offset.getStartOffset());
    assertEquals(10, offset.getCurrentVectorSize());

    offset.advance();
    assertFalse(offset.isDone());
    assertEquals(10, offset.getStartOffset());
    assertEquals(5, offset.getCurrentVectorSize());

    offset.advance();
    assertTrue(offset.isDone());

    offset.reset();
    assertFalse(offset.isDone());
    offset.advance();
    assertFalse(offset.isDone());
    offset.advance();
    assertTrue(offset.isDone());
  }

  // Granularizer tests to validate the expected protocol. For each, tests
  // the equlvalent time group iteratior implementation.

  /**
   * Segment covers one hour, query is for one hour, but a different day.
   */
  @Test
  public void testGranularizerNoOverlap()
  {
    MockVectorCursor cursor = new MockVectorCursor(10);
    VectorCursorGranularizer gran = VectorCursorGranularizer.create(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("HOUR"),
        Intervals.of("2015-09-13T13:00:00.000Z/2015-09-13T14:00:00.000Z")
    );
    assertNull(gran);
  }

  @Test
  public void testIteratorNoOverlap()
  {
    MockVectorCursor cursor = new MockVectorCursor(10);
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("HOUR"),
        Intervals.of("2015-09-13T13:00:00.000Z/2015-09-13T14:00:00.000Z"),
        false
    );
    assertFalse(iter.nextGroup());
    cursor.close();
  }

  /**
   * Query covers same hour as segment, but segment is empty.
   */
  @Test
  public void testGranularizerEmptySegment()
  {
    MockVectorCursor cursor = new MockVectorCursor(0);
    assertTrue(cursor.isDone());
    cursor.close();
  }

  @Test
  public void testIteratorEmptySegment()
  {
    MockVectorCursor cursor = new MockVectorCursor(0);
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("HOUR"),
        Intervals.of("2015-09-13T13:00:00.000Z/2015-09-13T14:00:00.000Z"),
        false
    );
    assertFalse(iter.nextGroup());
    cursor.close();
  }

  /**
   * Simplest case with data: one row which requires one vector.
   */
  @Test
  public void testGranularizerOneIntervalOneRow()
  {
    MockVectorCursor cursor = new MockVectorCursor(1);
    assertFalse(cursor.isDone());
    VectorCursorGranularizer gran = VectorCursorGranularizer.create(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("HOUR"),
        MockStorageAdapter.MOCK_INTERVAL
    );
    assertNotNull(gran);

    Iterator<Interval> iter = gran.getBucketIterable().iterator();

    // First bucket interval
    assertTrue(iter.hasNext());
    Interval interval = iter.next();
    assertEquals(MockStorageAdapter.MOCK_INTERVAL, interval);
    gran.setCurrentOffsets(interval);
    assertTrue(gran.advanceCursorWithinBucket());
    assertEquals(0, gran.getStartOffset());
    assertEquals(1, gran.getEndOffset());

    // No more data
    assertTrue(cursor.isDone());
  }

  @Test
  public void testIteratorOneIntervalOneRow()
  {
    MockVectorCursor cursor = new MockVectorCursor(1);
    assertFalse(cursor.isDone());
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("HOUR"),
        MockStorageAdapter.MOCK_INTERVAL,
        false
    );
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(1, iter.endOffset());

    // No more data
    assertFalse(iter.nextBatch());
    assertFalse(iter.nextGroup());
  }

  /**
   * Test a segment that matches the vector length.
   */
  @Test
  public void testGranularizerOneIntervalOneBatch()
  {
    MockVectorCursor cursor = new MockVectorCursor(BATCH_SIZE);
    assertFalse(cursor.isDone());
    VectorCursorGranularizer gran = VectorCursorGranularizer.create(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("HOUR"),
        MockStorageAdapter.MOCK_INTERVAL
    );
    assertNotNull(gran);

    // First bucket interval
    Iterator<Interval> iter = gran.getBucketIterable().iterator();
    assertTrue(iter.hasNext());
    Interval interval = iter.next();
    assertEquals(MockStorageAdapter.MOCK_INTERVAL, interval);
    gran.setCurrentOffsets(interval);
    assertTrue(gran.advanceCursorWithinBucket());
    assertEquals(0, gran.getStartOffset());
    assertEquals(BATCH_SIZE, gran.getEndOffset());

    // No more data
    assertTrue(cursor.isDone());
  }

  @Test
  public void testIteratorOneIntervalOneBatch()
  {
    MockVectorCursor cursor = new MockVectorCursor(BATCH_SIZE);
    assertFalse(cursor.isDone());
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("HOUR"),
        MockStorageAdapter.MOCK_INTERVAL,
        false
    );
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(BATCH_SIZE, iter.endOffset());

    // No more data
    assertFalse(iter.nextBatch());
    assertFalse(iter.nextGroup());
  }

  /**
   * Segment large enough to require two batches.
   */

  @Test
  public void testOneIntervalTwoBatches()
  {
    MockVectorCursor cursor = new MockVectorCursor(BATCH_SIZE + 1);
    assertFalse(cursor.isDone());
    VectorCursorGranularizer gran = VectorCursorGranularizer.create(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("HOUR"),
        MockStorageAdapter.MOCK_INTERVAL
    );
    assertNotNull(gran);

    // First bucket interval
    Iterator<Interval> iter = gran.getBucketIterable().iterator();
    assertTrue(iter.hasNext());
    Interval interval = iter.next();
    assertEquals(MockStorageAdapter.MOCK_INTERVAL, interval);
    gran.setCurrentOffsets(interval);
    assertEquals(0, gran.getStartOffset());
    assertEquals(BATCH_SIZE, cursor.getCurrentVectorSize());
    assertEquals(BATCH_SIZE, gran.getEndOffset());

    // Second batch
    assertFalse(cursor.isDone());
    assertTrue(gran.advanceCursorWithinBucket());
    gran.setCurrentOffsets(interval);
    assertEquals(0, gran.getStartOffset());
    assertEquals(1, cursor.getCurrentVectorSize());
    assertEquals(1, gran.getEndOffset());

    // No more data
    assertTrue(gran.advanceCursorWithinBucket());
    assertTrue(cursor.isDone());
  }

  @Test
  public void testIteratorOneIntervalTwoBatches()
  {
    MockVectorCursor cursor = new MockVectorCursor(BATCH_SIZE + 1);
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("HOUR"),
        MockStorageAdapter.MOCK_INTERVAL,
        false
    );
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(BATCH_SIZE, iter.endOffset());

    // Second batch
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(1, iter.endOffset());

    // No more data
    assertFalse(iter.nextBatch());
    assertFalse(iter.nextGroup());
  }

  /**
   * Two contiguous time intervals that fit into a single vector.
   */
  @Test
  public void testTwoIntervalsOneBatch()
  {
    long base = MockStorageAdapter.MOCK_INTERVAL.getStartMillis();
    long oneMin = 60 * 1000;
    long[] times = {
        base,
        base,
        base + oneMin,
        base + oneMin
    };
    MockVectorCursor cursor = new MockVectorCursor(times, 5);
    assertFalse(cursor.isDone());
    VectorCursorGranularizer gran = VectorCursorGranularizer.create(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("MINUTE"),
        Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T13:02:00.000Z")
    );
    assertNotNull(gran);

    // First bucket interval
    Iterator<Interval> iter = gran.getBucketIterable().iterator();
    assertTrue(iter.hasNext());
    Interval interval = iter.next();
    assertEquals(Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T13:01:00.000Z"), interval);
    gran.setCurrentOffsets(interval);
    assertEquals(0, gran.getStartOffset());
    assertEquals(4, cursor.getCurrentVectorSize());
    assertEquals(2, gran.getEndOffset());

    // End of first interval
    assertFalse(gran.advanceCursorWithinBucket());

    // Second interval
    assertFalse(cursor.isDone());
    assertTrue(iter.hasNext());
    interval = iter.next();
    assertEquals(Intervals.of("2015-09-12T13:01:00.000Z/2015-09-12T13:02:00.000Z"), interval);
    gran.setCurrentOffsets(interval);
    assertEquals(2, gran.getStartOffset());
    assertEquals(4, cursor.getCurrentVectorSize());
    assertEquals(4, gran.getEndOffset());

    // No more data
    assertTrue(gran.advanceCursorWithinBucket());
    assertTrue(cursor.isDone());
  }

  @Test
  public void testIteratorTwoIntervalsOneBatch()
  {
    long base = MockStorageAdapter.MOCK_INTERVAL.getStartMillis();
    long oneMin = 60 * 1000;
    long[] times = {
        base,
        base,
        base + oneMin,
        base + oneMin
    };
    MockVectorCursor cursor = new MockVectorCursor(times, 5);
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("MINUTE"),
        Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T13:02:00.000Z"),
        false
    );

    // First interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Second interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(2, iter.startOffset());
    assertEquals(4, iter.endOffset());
    assertFalse(iter.nextBatch());

    // No more data
    assertFalse(iter.nextBatch());
    assertFalse(iter.nextGroup());
  }

  /**
   * Three intervals split over two matches with even boundaries.
   */
  @Test
  public void testIteratorThreeIntervalsTwoBatches()
  {
    long base = MockStorageAdapter.MOCK_INTERVAL.getStartMillis();
    long oneMin = 60 * 1000;
    long[] times = {
        base,
        base,
        base + oneMin,
        base + oneMin,
        base + 2 * oneMin,
        base + 2 * oneMin
    };
    MockVectorCursor cursor = new MockVectorCursor(times, 4);
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("MINUTE"),
        Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T13:03:00.000Z"),
        false
    );

    // First interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Second interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(2, iter.startOffset());
    assertEquals(4, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Third interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    assertFalse(iter.nextBatch());

    // No more data
    assertFalse(iter.nextBatch());
    assertFalse(iter.nextGroup());
  }

  /**
   * As above, but the middle group is split across two vectors.
   */
  @Test
  public void testIteratorThreeIntervalsTwoBatchesSplit()
  {
    long base = MockStorageAdapter.MOCK_INTERVAL.getStartMillis();
    long oneMin = 60 * 1000;
    long[] times = {
        base,
        base,
        base + oneMin,
        base + oneMin,
        base + 2 * oneMin,
        base + 2 * oneMin
    };
    MockVectorCursor cursor = new MockVectorCursor(times, 3);
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("MINUTE"),
        Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T13:03:00.000Z"),
        false
    );

    // First interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Second interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(2, iter.startOffset());
    assertEquals(3, iter.endOffset());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(1, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Third interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(1, iter.startOffset());
    assertEquals(3, iter.endOffset());
    assertFalse(iter.nextBatch());

    // No more data
    assertFalse(iter.nextBatch());
    assertFalse(iter.nextGroup());
  }

  /**
   * Corner cases where the caller doesn't advance the vectors over all of a group,
   * and the task is left to the next group advancement.
   */
  @Test
  public void testIteratorThreeIntervalsTwoBatchesPartial()
  {
    long base = MockStorageAdapter.MOCK_INTERVAL.getStartMillis();
    long oneMin = 60 * 1000;
    long[] times = {
        base,
        base,
        base + oneMin,
        base + oneMin,
        base + 2 * oneMin,
        base + 2 * oneMin
    };
    MockVectorCursor cursor = new MockVectorCursor(times, 4);
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("MINUTE"),
        Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T13:03:00.000Z"),
        false
    );

    // First interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    // Don't advance the batch

    // Second interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(2, iter.startOffset());
    assertEquals(4, iter.endOffset());
    // Don't advance the batch

    // Third interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    // Don't advance the batch

    // No more data
    assertFalse(iter.nextBatch());
    assertFalse(iter.nextGroup());
  }

  /**
   * Empty intervals - skipped.
   */
  @Test
  public void testIteratorSkipEmptyIntervals()
  {
    long base = MockStorageAdapter.MOCK_INTERVAL.getStartMillis();
    long oneMin = 60 * 1000;
    long[] times = {
        // Empty at start
        base + oneMin,
        base + oneMin,
        // Empty in middle
        base + 3 * oneMin,
        base + 3 * oneMin,
        // Empty at start of vector
        base + 5 * oneMin,
        base + 5 * oneMin
        // Empty at end
    };
    MockVectorCursor cursor = new MockVectorCursor(times, 4);
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("MINUTE"),
        Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T13:07:00.000Z"),
        false
    );

    // First interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Second interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(2, iter.startOffset());
    assertEquals(4, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Third interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    assertFalse(iter.nextBatch());

    // No more data
    assertFalse(iter.nextBatch());
    assertFalse(iter.nextGroup());
  }

  /**
   * Empty intervals - retained
   */
  @Test
  public void testIteratorIncludeEmptyIntervals()
  {
    long base = MockStorageAdapter.MOCK_INTERVAL.getStartMillis();
    long oneMin = 60 * 1000;
    long[] times = {
        // Empty at start
        base + oneMin,
        base + oneMin,
        // Empty in middle
        base + 3 * oneMin,
        base + 3 * oneMin,
        // Empty at start of vector
        base + 5 * oneMin,
        base + 5 * oneMin
        // Empty at end
    };
    MockVectorCursor cursor = new MockVectorCursor(times, 4);
    TimeGroupVectorIterator iter = new TimeGroupVectorIterator(
        new MockStorageAdapter(),
        cursor,
        Granularity.fromString("MINUTE"),
        Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T13:07:00.000Z"),
        true
    );

    // First (empty) interval
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(0, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Second interval (data)
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Third interval (empty)
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(0, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Fourth interval (data)
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(2, iter.startOffset());
    assertEquals(4, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Fifth interval (empty)
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(0, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Sixth interval (data)
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(2, iter.endOffset());
    assertFalse(iter.nextBatch());

    // Seventh interval (empty)
    assertTrue(iter.nextGroup());
    assertTrue(iter.nextBatch());
    assertEquals(0, iter.startOffset());
    assertEquals(0, iter.endOffset());
    assertFalse(iter.nextBatch());

    // No more data
    assertFalse(iter.nextBatch());
    assertFalse(iter.nextGroup());
  }
}
