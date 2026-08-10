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

package org.apache.druid.segment.vector;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.List;
import java.util.stream.Stream;

/**
 * Verifies the tail-first, ascending-within-batch behavior of {@link DescendingNoFilterVectorOffset} and
 * {@link DescendingBitmapVectorOffset}. Combined with the value reversal done by
 * {@link ReverseVectorColumnSelectorFactory}, iterating these offsets yields rows in fully descending order.
 */
public class DescendingVectorOffsetTest
{
  private static final int BITMAP_ROWS = 1000;

  private static final List<Integer> NO_FILTER_VECTOR_SIZES = List.of(1, 2, 3, 7, 16);
  private static final List<Integer> NO_FILTER_ENDS = List.of(0, 1, 5, 16, 17, 100);
  private static final List<Integer> NO_FILTER_STARTS = List.of(0, 1, 3);
  private static final List<Integer> BITMAP_VECTOR_SIZES = List.of(1, 2, 7, 16);

  /**
   * Every bit set, and every third one.
   */
  private static final List<Integer> BITMAP_STEPS = List.of(1, 3);

  /**
   * The whole bitmap, an interior range, a single-row range, the last row, and two empty ranges.
   */
  private static final List<int[]> BITMAP_RANGES = List.of(
      new int[]{0, BITMAP_ROWS},
      new int[]{10, 500},
      new int[]{7, 8},
      new int[]{999, 1000},
      new int[]{500, 500},
      new int[]{0, 0}
  );

  public static Stream<Arguments> noFilterParameters()
  {
    return NO_FILTER_VECTOR_SIZES.stream().flatMap(
        vectorSize -> NO_FILTER_ENDS.stream().flatMap(
            end -> NO_FILTER_STARTS.stream()
                                   .filter(start -> start <= end)
                                   .map(start -> Arguments.of(vectorSize, start, end))
        )
    );
  }

  public static Stream<Arguments> bitmapParameters()
  {
    return BITMAP_STEPS.stream().flatMap(
        step -> BITMAP_VECTOR_SIZES.stream().flatMap(
            vectorSize -> BITMAP_RANGES.stream().map(range -> Arguments.of(step, vectorSize, range[0], range[1]))
        )
    );
  }

  @ParameterizedTest(name = "vectorSize[{0}] range[{1},{2})")
  @MethodSource("noFilterParameters")
  public void testNoFilterProducesDescendingOrder(final int vectorSize, final int start, final int end)
  {
    final DescendingNoFilterVectorOffset offset = new DescendingNoFilterVectorOffset(vectorSize, start, end);

    // Reversing each batch's ascending range and concatenating across batches must yield [end-1 .. start].
    final IntList logical = new IntArrayList();
    while (!offset.isDone()) {
      Assertions.assertTrue(offset.isContiguous(), "isContiguous");
      Assertions.assertThrows(UnsupportedOperationException.class, offset::getOffsets);

      final int startOffset = offset.getStartOffset();
      final int size = offset.getCurrentVectorSize();
      Assertions.assertTrue(size > 0 && size <= vectorSize, "0 < size[" + size + "] <= vectorSize");

      // Batches are aligned to multiples of vectorSize, so that they do not straddle compressed block boundaries.
      // Only the batch that runs into "start" may be unaligned.
      Assertions.assertTrue(
          startOffset == start || startOffset % vectorSize == 0,
          "aligned startOffset[" + startOffset + "]"
      );

      for (int i = size - 1; i >= 0; i--) {
        logical.add(startOffset + i);
      }
      offset.advance();
    }

    final IntList expected = new IntArrayList();
    for (int i = end - 1; i >= start; i--) {
      expected.add(i);
    }
    Assertions.assertEquals(expected, logical);
  }

  @ParameterizedTest(name = "step[{0}] vectorSize[{1}] range[{2},{3})")
  @MethodSource("bitmapParameters")
  public void testBitmapProducesDescendingOrder(
      final int step,
      final int vectorSize,
      final int start,
      final int end
  )
  {
    final ImmutableBitmap bitmap = makeBitmap(BITMAP_ROWS, step);
    final DescendingBitmapVectorOffset offset = new DescendingBitmapVectorOffset(vectorSize, bitmap, start, end);

    final IntList logical = new IntArrayList();
    while (!offset.isDone()) {
      final int size = offset.getCurrentVectorSize();
      Assertions.assertTrue(size > 0 && size <= vectorSize, "0 < size[" + size + "] <= vectorSize");

      final int[] batch = currentBatch(offset);

      // Within a batch, offsets are ascending (required by column readers).
      for (int i = 1; i < size; i++) {
        Assertions.assertTrue(batch[i] > batch[i - 1], "ascending within batch");
      }

      // getId is the smallest offset of the batch.
      Assertions.assertEquals(batch[0], offset.getId(), "getId");

      for (int i = size - 1; i >= 0; i--) {
        logical.add(batch[i]);
      }
      offset.advance();
    }

    // Expected: matching set bits in [start, end) in descending order.
    final IntList expected = new IntArrayList();
    for (int i = end - 1; i >= start; i--) {
      if (bitmap.get(i)) {
        expected.add(i);
      }
    }
    Assertions.assertEquals(expected, logical);
  }

  @Test
  public void testNoFilterReset()
  {
    final DescendingNoFilterVectorOffset offset = new DescendingNoFilterVectorOffset(4, 0, 10);
    final IntList firstPass = drainNoFilter(offset);
    offset.reset();
    final IntList secondPass = drainNoFilter(offset);
    Assertions.assertEquals(firstPass, secondPass);
  }

  @Test
  public void testBitmapReset()
  {
    final ImmutableBitmap bitmap = makeBitmap(100, 3);
    final DescendingBitmapVectorOffset offset = new DescendingBitmapVectorOffset(7, bitmap, 5, 90);
    final IntList firstPass = drainBitmap(offset);
    offset.reset();
    final IntList secondPass = drainBitmap(offset);
    Assertions.assertEquals(firstPass, secondPass);
    Assertions.assertFalse(firstPass.isEmpty());
  }

  @Test
  public void testBitmapDetectsContiguousBatches()
  {
    final DescendingBitmapVectorOffset offset = new DescendingBitmapVectorOffset(4, makeBitmap(100, 1), 0, 100);
    Assertions.assertTrue(offset.isContiguous());
    Assertions.assertEquals(96, offset.getStartOffset());
    Assertions.assertEquals(4, offset.getCurrentVectorSize());
    Assertions.assertThrows(DruidException.class, offset::getOffsets);
  }

  private static ImmutableBitmap makeBitmap(final int rows, final int step)
  {
    final MutableRoaringBitmap wrapped = new MutableRoaringBitmap();
    for (int i = 0; i < rows; i++) {
      if (i % step == 0) {
        wrapped.add(i);
      }
    }
    return new WrappedImmutableRoaringBitmap(wrapped.toImmutableRoaringBitmap());
  }

  private static int[] currentBatch(final VectorOffset offset)
  {
    final int size = offset.getCurrentVectorSize();
    final int[] batch = new int[size];
    if (offset.isContiguous()) {
      Assertions.assertThrows(DruidException.class, offset::getOffsets);
      final int startOffset = offset.getStartOffset();
      for (int i = 0; i < size; i++) {
        batch[i] = startOffset + i;
      }
    } else {
      Assertions.assertThrows(DruidException.class, offset::getStartOffset);
      System.arraycopy(offset.getOffsets(), 0, batch, 0, size);
    }
    return batch;
  }

  private static IntList drainNoFilter(final DescendingNoFilterVectorOffset offset)
  {
    final IntList logical = new IntArrayList();
    while (!offset.isDone()) {
      final int startOffset = offset.getStartOffset();
      final int size = offset.getCurrentVectorSize();
      for (int i = size - 1; i >= 0; i--) {
        logical.add(startOffset + i);
      }
      offset.advance();
    }
    return logical;
  }

  private static IntList drainBitmap(final DescendingBitmapVectorOffset offset)
  {
    final IntList logical = new IntArrayList();
    while (!offset.isDone()) {
      final int[] batch = currentBatch(offset);
      for (int i = batch.length - 1; i >= 0; i--) {
        logical.add(batch[i]);
      }
      offset.advance();
    }
    return logical;
  }
}
