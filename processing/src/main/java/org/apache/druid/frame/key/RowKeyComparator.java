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

package org.apache.druid.frame.key;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.frame.read.FrameReaderUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Comparator for {@link RowKey} instances.
 *
 * Comparison logic in this class is very similar to {@link FrameComparisonWidget}, but is different because it works
 * on byte[] instead of Frames.
 */
public class RowKeyComparator implements Comparator<RowKey>
{
  private final int firstFieldPosition;
  private final int[] ascDescRunLengths;

  private RowKeyComparator(
      final int firstFieldPosition,
      final int[] ascDescRunLengths
  )
  {
    this.firstFieldPosition = firstFieldPosition;
    this.ascDescRunLengths = ascDescRunLengths;
  }

  public static RowKeyComparator create(final List<SortColumn> keyColumns)
  {
    return new RowKeyComparator(
        computeFirstFieldPosition(keyColumns.size()),
        computeAscDescRunLengths(keyColumns)
    );
  }

  /**
   * Compute the offset into each key where the first field starts.
   *
   * Public so {@link FrameComparisonWidgetImpl} can use it.
   */
  public static int computeFirstFieldPosition(final int fieldCount)
  {
    return Ints.checkedCast((long) fieldCount * Integer.BYTES);
  }

  /**
   * Given a list of sort columns, compute an array of the number of ascending fields in a run, followed by number of
   * descending fields in a run, followed by ascending, etc. For example: ASC, ASC, DESC, ASC would return [2, 1, 1]
   * and DESC, DESC, ASC would return [0, 2, 1].
   *
   * Public so {@link FrameComparisonWidgetImpl} can use it.
   */
  public static int[] computeAscDescRunLengths(final List<SortColumn> sortColumns)
  {
    final IntList ascDescRunLengths = new IntArrayList(4);

    boolean descending = false;
    int runLength = 0;

    for (final SortColumn column : sortColumns) {
      if (column.descending() != descending) {
        ascDescRunLengths.add(runLength);
        runLength = 0;
        descending = !descending;
      }

      runLength++;
    }

    if (runLength > 0) {
      ascDescRunLengths.add(runLength);
    }

    return ascDescRunLengths.toIntArray();
  }

  @Override
  @SuppressWarnings("SubtractionInCompareTo")
  public int compare(final RowKey key1, final RowKey key2)
  {
    // Similar logic to FrameComparaisonWidgetImpl, but implementation is different enough that we need our own.
    // Major difference is Frame v. Frame instead of byte[] v. byte[].

    final byte[] keyArray1 = key1.array();
    final byte[] keyArray2 = key2.array();

    int comparableBytesStartPosition1 = firstFieldPosition;
    int comparableBytesStartPosition2 = firstFieldPosition;

    boolean ascending = true;
    int field = 0;

    for (int numFields : ascDescRunLengths) {
      if (numFields > 0) {
        final int nextField = field + numFields;
        final int comparableBytesEndPosition1 = RowKeyReader.fieldEndPosition(keyArray1, nextField - 1);
        final int comparableBytesEndPosition2 = RowKeyReader.fieldEndPosition(keyArray2, nextField - 1);

        int cmp = FrameReaderUtils.compareByteArraysUnsigned(
            keyArray1,
            comparableBytesStartPosition1,
            comparableBytesEndPosition1 - comparableBytesStartPosition1,
            keyArray2,
            comparableBytesStartPosition2,
            comparableBytesEndPosition2 - comparableBytesStartPosition2
        );

        if (cmp != 0) {
          return ascending ? cmp : -cmp;
        }

        field = nextField;
        comparableBytesStartPosition1 = comparableBytesEndPosition1;
        comparableBytesStartPosition2 = comparableBytesEndPosition2;
      }

      ascending = !ascending;
    }

    return 0;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowKeyComparator that = (RowKeyComparator) o;
    return firstFieldPosition == that.firstFieldPosition
           && Arrays.equals(ascDescRunLengths, that.ascDescRunLengths);
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(firstFieldPosition);
    result = 31 * result + Arrays.hashCode(ascDescRunLengths);
    return result;
  }

  @Override
  public String toString()
  {
    return "RowKeyComparator{" +
           "firstFieldPosition=" + firstFieldPosition +
           ", ascDescRunLengths=" + Arrays.toString(ascDescRunLengths) +
           '}';
  }
}
