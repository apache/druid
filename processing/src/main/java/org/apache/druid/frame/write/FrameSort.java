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

package org.apache.druid.frame.write;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.key.FrameComparisonWidget;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.java.util.common.ISE;

import java.util.Arrays;
import java.util.List;

/**
 * Utility for sorting frames in-place.
 */
public class FrameSort
{
  private FrameSort()
  {
    // No instantiation.
  }

  /**
   * Sort the given frame in-place. It must be a permuted frame.
   *
   * @param frame       frame to sort
   * @param frameReader frame reader
   * @param sortColumns list of columns to sort by; must be nonempty.
   */
  public static void sort(
      final Frame frame,
      final FrameReader frameReader,
      final List<SortColumn> sortColumns
  )
  {
    if (!frame.isPermuted()) {
      throw new ISE("Cannot sort nonpermuted frame");
    }

    if (sortColumns.isEmpty()) {
      throw new ISE("Cannot sort with an empty column list");
    }

    final WritableMemory memory = frame.writableMemory();

    // Object array instead of int array, since we want timsort from Arrays.sort. (It does fewer comparisons
    // on partially-sorted data, which is common.)
    final Integer[] rows = new Integer[frame.numRows()];
    for (int i = 0; i < frame.numRows(); i++) {
      rows[i] = i;
    }

    final FrameComparisonWidget comparisonWidget1 = frameReader.makeComparisonWidget(frame, sortColumns);
    final FrameComparisonWidget comparisonWidget2 = frameReader.makeComparisonWidget(frame, sortColumns);

    Arrays.sort(
        rows,
        (k1, k2) -> comparisonWidget1.compare(k1, comparisonWidget2, k2)
    );

    // Replace logical row numbers with physical row numbers.
    for (int i = 0; i < frame.numRows(); i++) {
      rows[i] = frame.physicalRow(rows[i]);
    }

    // And copy them into the frame.
    for (int i = 0; i < frame.numRows(); i++) {
      memory.putInt(Frame.HEADER_SIZE + (long) i * Integer.BYTES, rows[i]);
    }
  }
}
