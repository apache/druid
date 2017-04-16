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

import io.druid.java.util.common.io.Closer;

import java.io.IOException;
import java.nio.IntBuffer;
import java.util.List;

/**
 * Processing related interface
 *
 * A DimensionMerger is a per-dimension stateful object that encapsulates type-specific operations and data structures
 * used during the segment merging process (i.e., work done by IndexMerger/IndexMergerV9).
 *
 * This object is responsible for:
 *   - merging any relevant structures from the segments (e.g., encoding dictionaries)
 *   - writing the merged column data and any merged indexing structures (e.g., dictionaries, bitmaps) to disk
 *
 * At a high level, the index merging process can be broken down into the following steps:
 *   - Merge any value representation metadata across segments
 *     E.g. for dictionary encoded Strings, each segment has its own unique space of id->value mappings.
 *     These need to be merged across segments into a shared space of dictionary mappings.
 *
 *   - Merge the rows across segments into a common sequence of rows
 *
 *   - After constructing the merged sequence of rows, build any applicable index structures (e.g, bitmap indexes)
 *
 *   - Write the value representation metadata (e.g. dictionary), the sequence of row values,
 *     and index structures to a merged segment.
 *
 * A class implementing this interface is expected to be highly stateful, updating its internal state as these
 * functions are called.
 *
 * @param <EncodedKeyComponentType> A row key contains a component for each dimension, this param specifies the
 *                                 class of this dimension's key component. A column type that supports multivalue rows
 *                                 should use an array type (Strings would use int[]). Column types without multivalue
 *                                 row support should use single objects (e.g., Long, Float).
 */
public interface DimensionMerger<EncodedKeyComponentType>
{
  /**
   * Given a list of segment adapters:
   * - Read any value metadata (e.g., dictionary encoding information) from the adapters
   * - Merge this value metadata and update the internal state of the implementing class.
   *
   * The implementer should maintain knowledge of the "index number" of the adapters in the input list,
   * i.e., the position of each adapter in the input list.
   *
   * This "index number" will be used to refer to specific segments later
   * in convertSegmentRowValuesToMergedRowValues().
   *
   * Otherwise, the details of how this merging occurs and how to store the merged data is left to the implementer.
   *
   * @param adapters List of adapters to be merged.
   * @throws IOException
   */
  void writeMergedValueMetadata(List<IndexableAdapter> adapters) throws IOException;


  /**
   * Convert a row's key component with per-segment encoding to its equivalent representation
   * in the merged set of rows.
   *
   * This function is used by the index merging process to build the merged sequence of rows.
   *
   * The implementing class is expected to use the merged value metadata constructed
   * during writeMergedValueMetadata, if applicable.
   *
   * For example, an implementation of this function for a dictionary-encoded String column would convert the
   * segment-specific dictionary values within the row to the common merged dictionary values
   * determined during writeMergedValueMetadata().
   *
   * @param segmentRow A row's key component for this dimension. The encoding of the key component's
   *                   values will be converted from per-segment encodings to the combined encodings from
   *                   the merged sequence of rows.
   * @param segmentIndexNumber Integer indicating which segment the row originated from.
   */
  EncodedKeyComponentType convertSegmentRowValuesToMergedRowValues(
      EncodedKeyComponentType segmentRow,
      int segmentIndexNumber
  );


  /**
   * Process a key component from the merged sequence of rows and update the DimensionMerger's internal state.
   *
   * After constructing a merged sequence of rows across segments, the index merging process will
   * iterate through these rows and pass row key components from each dimension to their corresponding DimensionMergers.
   *
   * This allows each DimensionMerger to build its internal view of the sequence of merged rows, to be
   * written out to a segment later.
   *
   * @param rowValues The row values to be added.
   * @throws IOException
   */
  void processMergedRow(EncodedKeyComponentType rowValues) throws IOException;


  /**
   * Internally construct any index structures relevant to this DimensionMerger.
   *
   * After receiving the sequence of merged rows via iterated processMergedRow() calls, the DimensionMerger
   * can now build any index structures it needs.
   *
   * For example, a dictionary encoded String implementation would create its bitmap indexes
   * for the merged segment during this step.
   *
   * The index merger will provide a list of row number conversion IntBuffer objects.
   * Each IntBuffer is associated with one of the segments being merged; the position of the IntBuffer in the list
   * corresponds to the position of segment adapters within the input list of writeMergedValueMetadata().
   *
   * For example, suppose there are two segments A and B.
   * Row 24 from segment A maps to row 99 in the merged sequence of rows,
   * The IntBuffer for segment A would have a mapping of 24 -> 99.
   *
   * @param segmentRowNumConversions A list of row number conversion IntBuffer objects.
   * @param closer Add Closeables for resource cleanup to this Closer if needed
   * @throws IOException
   */
  void writeIndexes(List<IntBuffer> segmentRowNumConversions, Closer closer) throws IOException;


  /**
   * Return true if this dimension's data does not need to be written to the segment.
   *
   * For example, if a dictionary-encoded String dimension had only null values, it can be skipped.
   *
   * @return true if this dimension can be excluded from the merged segment.
   */
  boolean canSkip();
}
