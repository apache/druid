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

package org.apache.druid.query.aggregation;

import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Direct unit tests for the {@code elseValue} path of {@link FilteredAggregator},
 * {@link FilteredBufferAggregator}, and {@link FilteredVectorAggregator}.
 */
@RunWith(Enclosed.class)
public class FilteredAggregatorElseValueTest
{
  private static final int VECTOR_SIZE = 8;

  /**
   * Bytes per slot for a {@link FilteredBufferAggregator} backed by {@link StubBufferAggregator}: 1 byte for the
   * wrapper's unmatched flag + 9 bytes of stub state.
   */
  private static final int SLOT_SIZE = 10;

  private static void copySlot(
      final ByteBuffer src,
      final int srcPos,
      final ByteBuffer dst,
      final int dstPos,
      final int len
  )
  {
    for (int i = 0; i < len; i++) {
      dst.put(dstPos + i, src.get(srcPos + i));
    }
  }

  public static class AggregatorTest extends InitializedNullHandlingTest
  {
    @Test
    public void testEmptyInputReturnsNull()
    {
      final StubAggregator delegate = new StubAggregator();
      final FilteredAggregator agg = new FilteredAggregator(new StubValueMatcher(), delegate, 0);

      Assert.assertTrue(agg.isNull());
      Assert.assertNull(agg.get());
    }

    @Test
    public void testAllMatchReturnsDelegateSum()
    {
      final StubAggregator delegate = new StubAggregator();
      final StubValueMatcher matcher = new StubValueMatcher(true, true, true);
      final FilteredAggregator agg = new FilteredAggregator(matcher, delegate, 0);

      for (int i = 0; i < 3; i++) {
        agg.aggregate();
      }

      Assert.assertFalse(agg.isNull());
      Assert.assertEquals(3L, agg.getLong());
      Assert.assertEquals(3L, agg.get());
      Assert.assertEquals(3.0d, agg.getDouble(), 0.0);
      Assert.assertEquals(3.0f, agg.getFloat(), 0.0f);
    }

    @Test
    public void testAllUnmatchedWithElseValueReturnsZero()
    {
      final StubAggregator delegate = new StubAggregator();
      final StubValueMatcher matcher = new StubValueMatcher(false, false, false);
      final FilteredAggregator agg = new FilteredAggregator(matcher, delegate, 0);

      for (int i = 0; i < 3; i++) {
        agg.aggregate();
      }

      Assert.assertFalse(agg.isNull());
      Assert.assertEquals(0L, agg.getLong());
      Assert.assertEquals(0, agg.get());
      Assert.assertEquals(0.0d, agg.getDouble(), 0.0);
      Assert.assertEquals(0.0f, agg.getFloat(), 0.0f);
    }

    @Test
    public void testAllUnmatchedWithoutElseValueReturnsNull()
    {
      final StubAggregator delegate = new StubAggregator();
      final StubValueMatcher matcher = new StubValueMatcher(false, false, false);
      final FilteredAggregator agg = new FilteredAggregator(matcher, delegate, null);

      for (int i = 0; i < 3; i++) {
        agg.aggregate();
      }

      Assert.assertTrue(agg.isNull());
      Assert.assertNull(agg.get());
    }

    @Test
    public void testMixedWithElseValueReturnsMatchedSum()
    {
      final StubAggregator delegate = new StubAggregator();
      final StubValueMatcher matcher = new StubValueMatcher(true, false, true, false);
      final FilteredAggregator agg = new FilteredAggregator(matcher, delegate, 0);

      for (int i = 0; i < 4; i++) {
        agg.aggregate();
      }

      Assert.assertFalse(agg.isNull());
      Assert.assertEquals(2L, agg.getLong());
    }
  }

  public static class BufferAggregatorTest extends InitializedNullHandlingTest
  {
    @Test
    public void testEmptyInputReturnsNull()
    {
      runScenario(0, null, new boolean[0]);
    }

    @Test
    public void testAllMatchReturnsDelegateSum()
    {
      runScenario(0, 3L, new boolean[]{true, true, true});
    }

    @Test
    public void testAllUnmatchedWithElseValueReturnsZero()
    {
      runScenario(0, 0L, new boolean[]{false, false, false});
    }

    @Test
    public void testAllUnmatchedWithoutElseValueReturnsNull()
    {
      runScenario(null, null, new boolean[]{false, false, false});
    }

    @Test
    public void testMixedWithElseValueReturnsMatchedSum()
    {
      runScenario(0, 2L, new boolean[]{true, false, true, false});
    }

    @Test
    public void testRelocatePreservesFlag()
    {
      final StubBufferAggregator delegate = new StubBufferAggregator();
      final FilteredBufferAggregator agg = new FilteredBufferAggregator(new StubValueMatcher(), delegate, 0);

      final ByteBuffer src = ByteBuffer.allocate(64);
      final int srcPos = 8;
      agg.init(src, srcPos);

      agg.aggregate(src, srcPos);

      // relocate() is a notification: the caller has already moved the slot bytes.
      final ByteBuffer dst = ByteBuffer.allocate(64);
      final int dstPos = 24;
      copySlot(src, srcPos, dst, dstPos, SLOT_SIZE);
      agg.relocate(srcPos, dstPos, src, dst);

      Assert.assertFalse(agg.isNull(dst, dstPos));
      Assert.assertEquals(0, agg.get(dst, dstPos));
      Assert.assertEquals(0L, agg.getLong(dst, dstPos));
    }

    private static void runScenario(
        @Nullable final Number elseValue,
        @Nullable final Long expectedValue,
        final boolean[] matchDecisions
    )
    {
      final StubBufferAggregator delegate = new StubBufferAggregator();
      final StubValueMatcher matcher = new StubValueMatcher(matchDecisions);
      final FilteredBufferAggregator agg = new FilteredBufferAggregator(matcher, delegate, elseValue);

      final ByteBuffer buf = ByteBuffer.allocate(32);
      final int position = 16;
      agg.init(buf, position);

      for (int i = 0; i < matchDecisions.length; i++) {
        agg.aggregate(buf, position);
      }

      Assert.assertEquals(expectedValue == null, agg.isNull(buf, position));
      if (expectedValue == null) {
        Assert.assertNull(agg.get(buf, position));
      } else {
        Assert.assertEquals(expectedValue.longValue(), ((Number) agg.get(buf, position)).longValue());
        Assert.assertEquals(expectedValue.longValue(), agg.getLong(buf, position));
      }
    }
  }

  public static class VectorAggregatorTest extends InitializedNullHandlingTest
  {
    @Test
    public void testAggregateRangeEmptyInputReturnsNull()
    {
      runRange(0, null, new boolean[0]);
    }

    @Test
    public void testAggregateRangeAllMatchReturnsDelegateSum()
    {
      runRange(0, 4L, new boolean[]{true, true, true, true});
    }

    @Test
    public void testAggregateRangeAllUnmatchedWithElseValueReturnsZero()
    {
      runRange(0, 0L, new boolean[]{false, false, false, false});
    }

    @Test
    public void testAggregateRangeAllUnmatchedWithoutElseValueReturnsNull()
    {
      runRange(null, null, new boolean[]{false, false, false, false});
    }

    @Test
    public void testAggregateRangeMixedWithElseValueReturnsMatchedSum()
    {
      runRange(0, 2L, new boolean[]{true, false, true, false});
    }

    @Test
    public void testAggregateRangePartialSliceAllMatchDoesNotSetFlag()
    {
      // Slice is rows [0,2) but matcher reports vector size 4, so isAllTrue(getCurrentVectorSize)
      // returns false even when the slice is fully matched. Flag must stay clear.
      final boolean[] matchedRows = {true, true, false, false};
      final FilteredVectorAggregator agg = newAggregator(matchedRows, 0);
      final ByteBuffer buf = ByteBuffer.allocate(32);
      final int position = 0;
      agg.init(buf, position);

      agg.aggregate(buf, position, 0, 2);

      Assert.assertEquals(2L, ((Number) agg.get(buf, position)).longValue());
      Assert.assertEquals(0, buf.get(position));
    }

    @Test
    public void testAggregateScatteredRowsNullMixedWithElseValue()
    {
      // Rows [0..3] interleaved between two slots. Even rows match (slot A); odd rows are unmatched (slot B).
      final boolean[] matchedRows = {true, false, true, false};
      final int[] positions = {0, 16, 0, 16};
      final FilteredVectorAggregator agg = newAggregator(matchedRows, 0);

      final ByteBuffer buf = ByteBuffer.allocate(32);
      agg.init(buf, positions[0]);
      agg.init(buf, positions[1]);

      agg.aggregate(buf, 4, positions, null, 0);

      Assert.assertEquals(2L, ((Number) agg.get(buf, positions[0])).longValue());
      Assert.assertEquals(0, agg.get(buf, positions[1]));
    }

    @Test
    public void testAggregateScatteredRowsExplicitMixedWithElseValue()
    {
      // Selected rows = [1, 3, 5, 7]; rows 3 and 5 match the filter, rows 1 and 7 do not.
      final boolean[] matchedRows = new boolean[VECTOR_SIZE];
      matchedRows[3] = true;
      matchedRows[5] = true;

      final FilteredVectorAggregator agg = newAggregator(matchedRows, 0);
      final ByteBuffer buf = ByteBuffer.allocate(32);
      final int slotA = 0;
      final int slotB = 16;
      agg.init(buf, slotA);
      agg.init(buf, slotB);

      // VectorMatch invariant: rows[] length must be at least the largest row id, so size it to the
      // vector and only populate the first numRows entries.
      final int[] positions = {slotA, slotB, slotA, slotB};
      final int[] rows = new int[VECTOR_SIZE];
      rows[0] = 1;
      rows[1] = 3;
      rows[2] = 5;
      rows[3] = 7;
      agg.aggregate(buf, 4, positions, rows, 0);

      Assert.assertEquals(1L, ((Number) agg.get(buf, slotA)).longValue());
      Assert.assertEquals(1L, ((Number) agg.get(buf, slotB)).longValue());
    }

    @Test
    public void testRelocatePreservesFlag()
    {
      final boolean[] matchedRows = new boolean[VECTOR_SIZE];
      final FilteredVectorAggregator agg = newAggregator(matchedRows, 0);

      final ByteBuffer src = ByteBuffer.allocate(64);
      final int srcPos = 8;
      agg.init(src, srcPos);
      agg.aggregate(src, srcPos, 0, 4);

      // relocate() is a notification: the caller has already moved the slot bytes.
      final ByteBuffer dst = ByteBuffer.allocate(64);
      final int dstPos = 24;
      copySlot(src, srcPos, dst, dstPos, SLOT_SIZE);
      agg.relocate(srcPos, dstPos, src, dst);

      Assert.assertEquals(0, agg.get(dst, dstPos));
    }

    private static void runRange(
        @Nullable final Number elseValue,
        @Nullable final Long expectedValue,
        final boolean[] matchedRows
    )
    {
      final FilteredVectorAggregator agg = newAggregator(matchedRows, elseValue);
      final ByteBuffer buf = ByteBuffer.allocate(32);
      final int position = 8;
      agg.init(buf, position);
      if (matchedRows.length > 0) {
        agg.aggregate(buf, position, 0, matchedRows.length);
      }

      if (expectedValue == null) {
        Assert.assertNull(agg.get(buf, position));
      } else {
        Assert.assertEquals(expectedValue.longValue(), ((Number) agg.get(buf, position)).longValue());
      }
    }

    private static FilteredVectorAggregator newAggregator(final boolean[] matchedRows, @Nullable final Number elseValue)
    {
      final StubVectorAggregator delegate = new StubVectorAggregator();
      final StubVectorValueMatcher matcher = new StubVectorValueMatcher(matchedRows);
      return new FilteredVectorAggregator(matcher, delegate, elseValue);
    }
  }

  /**
   * Matches one row at a time from a fixed list of decisions. Defaults to {@code false} once exhausted.
   */
  private static class StubValueMatcher implements ValueMatcher
  {
    private final boolean[] decisions;
    private int index;

    StubValueMatcher(final boolean... decisions)
    {
      this.decisions = decisions;
    }

    @Override
    public boolean matches(final boolean includeUnknown)
    {
      return index < decisions.length && decisions[index++];
    }

    @Override
    public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
    {
    }
  }

  /**
   * Counts each {@code aggregate()} call as a contribution of 1; reports null until the first call.
   */
  private static class StubAggregator implements Aggregator
  {
    private long sum;
    private boolean hasValue;

    @Override
    public void aggregate()
    {
      sum++;
      hasValue = true;
    }

    @Override
    public boolean isNull()
    {
      return !hasValue;
    }

    @Nullable
    @Override
    public Object get()
    {
      return hasValue ? sum : null;
    }

    @Override
    public float getFloat()
    {
      return sum;
    }

    @Override
    public long getLong()
    {
      return sum;
    }

    @Override
    public double getDouble()
    {
      return sum;
    }

    @Override
    public void close()
    {
    }
  }

  /**
   * Slot layout: [1 byte flag @ position][8 bytes sum @ position+1].
   */
  private static class StubBufferAggregator implements BufferAggregator
  {
    @Override
    public void init(final ByteBuffer buf, final int position)
    {
      buf.put(position, (byte) 0);
      buf.putLong(position + 1, 0L);
    }

    @Override
    public void aggregate(final ByteBuffer buf, final int position)
    {
      buf.put(position, (byte) 1);
      buf.putLong(position + 1, buf.getLong(position + 1) + 1);
    }

    @Nullable
    @Override
    public Object get(final ByteBuffer buf, final int position)
    {
      return isNull(buf, position) ? null : buf.getLong(position + 1);
    }

    @Override
    public boolean isNull(final ByteBuffer buf, final int position)
    {
      return buf.get(position) == 0;
    }

    @Override
    public float getFloat(final ByteBuffer buf, final int position)
    {
      return buf.getLong(position + 1);
    }

    @Override
    public long getLong(final ByteBuffer buf, final int position)
    {
      return buf.getLong(position + 1);
    }

    @Override
    public double getDouble(final ByteBuffer buf, final int position)
    {
      return buf.getLong(position + 1);
    }

    @Override
    public void relocate(
        final int oldPosition,
        final int newPosition,
        final ByteBuffer oldBuffer,
        final ByteBuffer newBuffer
    )
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
    {
    }
  }

  /**
   * Same slot layout as {@link StubBufferAggregator}; vector entry points add 1 per row.
   */
  private static class StubVectorAggregator implements VectorAggregator
  {
    @Override
    public void init(final ByteBuffer buf, final int position)
    {
      buf.put(position, (byte) 0);
      buf.putLong(position + 1, 0L);
    }

    @Override
    public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
    {
      final int rowsAggregated = endRow - startRow;
      if (rowsAggregated > 0) {
        buf.put(position, (byte) 1);
        buf.putLong(position + 1, buf.getLong(position + 1) + rowsAggregated);
      }
    }

    @Override
    public void aggregate(
        final ByteBuffer buf,
        final int numRows,
        final int[] positions,
        @Nullable final int[] rows,
        final int positionOffset
    )
    {
      for (int i = 0; i < numRows; i++) {
        final int slot = positions[i] + positionOffset;
        buf.put(slot, (byte) 1);
        buf.putLong(slot + 1, buf.getLong(slot + 1) + 1);
      }
    }

    @Nullable
    @Override
    public Object get(final ByteBuffer buf, final int position)
    {
      return buf.get(position) == 0 ? null : buf.getLong(position + 1);
    }

    @Override
    public void relocate(
        final int oldPosition,
        final int newPosition,
        final ByteBuffer oldBuffer,
        final ByteBuffer newBuffer
    )
    {
    }

    @Override
    public void close()
    {
    }
  }

  /**
   * Returns the subset of the mask whose absolute row indices are flagged in {@code matchedRows}.
   */
  private static class StubVectorValueMatcher implements VectorValueMatcher
  {
    private final boolean[] matchedRows;

    StubVectorValueMatcher(final boolean[] matchedRows)
    {
      this.matchedRows = matchedRows;
    }

    @Override
    public ReadableVectorMatch match(final ReadableVectorMatch mask, final boolean includeUnknown)
    {
      final int[] maskSelection = mask.getSelection();
      final int maskSize = mask.getSelectionSize();
      // Size to getMaxVectorSize() so VectorMatch's invariant (largest row id <= array length) holds for
      // sparse selections like rows={1,3,5,7}.
      final int[] out = new int[getMaxVectorSize()];
      int n = 0;
      for (int i = 0; i < maskSize; i++) {
        final int rowNum = maskSelection[i];
        if (rowNum < matchedRows.length && matchedRows[rowNum]) {
          out[n++] = rowNum;
        }
      }
      return VectorMatch.wrap(out).setSelectionSize(n);
    }

    @Override
    public int getMaxVectorSize()
    {
      return Math.max(matchedRows.length, VECTOR_SIZE);
    }

    @Override
    public int getCurrentVectorSize()
    {
      return matchedRows.length;
    }
  }
}
