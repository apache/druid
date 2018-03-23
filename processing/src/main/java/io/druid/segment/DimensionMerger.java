/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.List;

/**
 * Processing related interface
 *
 * A DimensionMerger is a per-dimension stateful object that encapsulates type-specific operations and data structures
 * used during the segment merging process (i.e., work done by {@link IndexMerger}).
 *
 * This object is responsible for:
 *   - merging encoding dictionaries, if present
 *   - writing the merged column data and any merged indexing structures (e.g., dictionaries, bitmaps) to disk
 *
 * At a high level, the index merging process can be broken down into the following steps:
 *   - Merge segment's encoding dictionaries. These need to be merged across segments into a shared space of dictionary
 *     mappings: {@link #writeMergedValueDictionary(List)}.
 *
 *   - Merge the rows across segments into a common sequence of rows. Done outside of scope of this interface,
 *     currently in {@link IndexMergerV9}.
 *
 *   - After constructing the merged sequence of rows, process each individual row via {@link #processMergedRow},
 *     potentially continuing updating the internal structures.
 *
 *   - Write the value representation metadata (dictionary, bitmaps), the sequence of row values,
 *     and index structures to a merged segment: {@link #writeIndexes}
 *
 * A class implementing this interface is expected to be highly stateful, updating its internal state as these
 * functions are called.
 */
public interface DimensionMerger
{
  /**
   * Given a list of segment adapters:
   * - Read _sorted order_ (e. g. see {@link
   *   io.druid.segment.incremental.IncrementalIndexAdapter#getDimValueLookup(String)}) dictionary encoding information
   *   from the adapters
   * - Merge those sorted order dictionary into a one big sorted order dictionary and write this merged dictionary.
   *
   * The implementer should maintain knowledge of the "index number" of the adapters in the input list,
   * i.e., the position of each adapter in the input list.
   *
   * This "index number" will be used to refer to specific segments later
   * in {@link #convertSortedSegmentRowValuesToMergedRowValues}.
   *
   * @param adapters List of adapters to be merged.
   * @see DimensionIndexer#convertUnsortedValuesToSorted
   */
  void writeMergedValueDictionary(List<IndexableAdapter> adapters) throws IOException;

  /**
   * Creates a value selector, which converts values with per-segment, _sorted order_ (see {@link
   * DimensionIndexer#convertUnsortedValuesToSorted}) encoding from the given selector to their equivalent
   * representation in the merged set of rows.
   *
   * This method is used by the index merging process to build the merged sequence of rows.
   *
   * The implementing class is expected to use the merged value metadata constructed
   * during {@link #writeMergedValueDictionary(List)}, if applicable.
   *
   * For example, an implementation of this function for a dictionary-encoded String column would convert the
   * segment-specific, sorted order dictionary values within the row to the common merged dictionary values
   * determined during {@link #writeMergedValueDictionary(List)}.
   *
   * @param segmentIndex indicates which segment the row originated from, in the order established in
   * {@link #writeMergedValueDictionary(List)}
   * @param source the selector from which to take values to convert
   * @return a selector with converted values
   */
  ColumnValueSelector convertSortedSegmentRowValuesToMergedRowValues(int segmentIndex, ColumnValueSelector source);

  /**
   * Process a column value(s) (potentially multi-value) of a row from the given selector and update the
   * DimensionMerger's internal state.
   *
   * After constructing a merged sequence of rows across segments, the index merging process will iterate through these
   * rows and on each iteration, for each column, pass the column value selector to the corresponding DimensionMerger.
   *
   * This allows each DimensionMerger to build its internal view of the sequence of merged rows, to be
   * written out to a segment later.
   */
  void processMergedRow(ColumnValueSelector selector) throws IOException;

  /**
   * Internally construct any index structures relevant to this DimensionMerger.
   *
   * After receiving the sequence of merged rows via iterated {@link #processMergedRow} calls, the DimensionMerger
   * can now build any index structures it needs.
   *
   * For example, a dictionary encoded String implementation would create its bitmap indexes
   * for the merged segment during this step.
   *
   * The index merger will provide a list of row number conversion IntBuffer objects.
   * Each IntBuffer is associated with one of the segments being merged; the position of the IntBuffer in the list
   * corresponds to the position of segment adapters within the input list of {@link #writeMergedValueDictionary(List)}.
   *
   * For example, suppose there are two segments A and B.
   * Row 24 from segment A maps to row 99 in the merged sequence of rows,
   * The IntBuffer for segment A would have a mapping of 24 -> 99.
   *
   * @param segmentRowNumConversions A list of row number conversion IntBuffer objects.
   */
  void writeIndexes(@Nullable List<IntBuffer> segmentRowNumConversions) throws IOException;

  /**
   * Return true if this dimension's data does not need to be written to the segment.
   *
   * For example, if a dictionary-encoded String dimension had only null values, it can be skipped.
   *
   * @return true if this dimension can be excluded from the merged segment.
   */
  boolean canSkip();
}
