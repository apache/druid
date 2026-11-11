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

package org.apache.druid.segment.filter;

import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.query.filter.vector.ReadableVectorMatch;
import org.apache.druid.query.filter.vector.VectorMatch;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class OrFilterVectorMatcherTest
{
  private static final int[] INDEXED_ROWS = new int[]{3, 10, 13, 21};

  public static Stream<Arguments> batches()
  {
    return Stream.of(
        Arguments.of("ascending", new int[][]{{1, 2, 3}, {10, 11, 12}, {13, 20}}, List.of(3, 10, 13)),
        Arguments.of("descendingRanges", new int[][]{{20, 21}, {10, 11}, {1, 3}}, List.of(21, 10, 3)),
        Arguments.of("overlappingRanges", new int[][]{{1, 10}, {2, 3, 11}, {12, 13}}, List.of(10, 3, 13))
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("batches")
  public void testMatchesIndexedRows(String name, int[][] batches, List<Integer> expected)
  {
    final MutableBitmap mutableBitmap = RoaringBitmapFactory.INSTANCE.makeEmptyMutableBitmap();
    for (final int row : INDEXED_ROWS) {
      mutableBitmap.add(row);
    }

    final BatchedVectorOffset offset = new BatchedVectorOffset(batches);
    final VectorValueMatcher matcher = OrFilter.convertIndexToVectorValueMatcher(
        offset,
        RoaringBitmapFactory.INSTANCE.makeImmutableBitmap(mutableBitmap)
    );

    final List<Integer> matched = new ArrayList<>();
    for (; offset.batchIndex < batches.length; offset.batchIndex++) {
      final ReadableVectorMatch match = matcher.match(VectorMatch.allTrue(offset.getCurrentVectorSize()), false);
      for (int i = 0; i < match.getSelectionSize(); i++) {
        matched.add(offset.getOffsets()[match.getSelection()[i]]);
      }
    }

    Assertions.assertEquals(expected, matched);
  }

  /**
   * A {@link ReadableVectorOffset} that hands back a predetermined list of batches. Each batch must be internally
   * ascending, but batches are otherwise unconstrained.
   */
  private static class BatchedVectorOffset implements ReadableVectorOffset
  {
    private final int[][] batches;
    private final int maxVectorSize;

    private int batchIndex = 0;

    BatchedVectorOffset(final int[][] batches)
    {
      this.batches = batches;
      int max = 0;
      for (final int[] batch : batches) {
        max = Math.max(max, batch.length);
      }
      this.maxVectorSize = max;
    }

    @Override
    public boolean isContiguous()
    {
      return false;
    }

    @Override
    public int getStartOffset()
    {
      throw new UnsupportedOperationException("not contiguous");
    }

    @Override
    public int[] getOffsets()
    {
      return batches[batchIndex];
    }

    @Override
    public int getId()
    {
      return batchIndex;
    }

    @Override
    public int getMaxVectorSize()
    {
      return maxVectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return batches[batchIndex].length;
    }
  }
}
