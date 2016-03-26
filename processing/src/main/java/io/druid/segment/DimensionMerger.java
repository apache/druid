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

import com.google.common.io.OutputSupplier;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.Buffer;
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
 * @param <EncodedType> class of the encoded values
 * @param <ActualType> class of the actual values
 */
public interface DimensionMerger<EncodedType extends Comparable<EncodedType>, ActualType extends Comparable<ActualType>>
{
  /**
   * Initialize any writer objects that can be instantiated before data from the segments has been processed/merged.
   * @throws IOException
   */
  public void initWriters() throws IOException;


  /**
   * Read the segments to be merged and combine their indexes/data structures internally.
   * (e.g., merging of dictionaries for String dimensions).
   *
   * @param adapters List of adapters to be merged.
   * @throws IOException
   */
  public void mergeAcrossSegments(List<IndexableAdapter> adapters) throws IOException;


  /**
   * Update the DimensionMerger's internal structures with row value(s) to be merged.
   *
   * An example implementation might be to add the values to an open column writer.
   *
   * @param values The row values to be added.
   * @throws IOException
   */
  public void addColumnValue(Comparable[] values) throws IOException;


  /**
   * Close any open writers and write merged data to disk.
   *
   * @param rowNumConversions A list of conversion buffers indexed by adapter number
   *                          The list will have the same order as the list of adapters passed to mergeAcrossSegments()
   *                          This buffer can be used to convert the row numbers from individual adapters.
   * @throws IOException
   */
  public void closeWriters(List<IntBuffer> rowNumConversions) throws IOException;


  /**
   * When merging multiple segments, it is sometimes necessary to convert "missing" values to "nulls".
   *
   * Suppose we have String dimensions DimA and DimB and two segments to be merged.
   *
   * Segment 1 has rows:
   *   {dimB = hello}
   *
   * Segment 2 has rows
   *   {dimB = hello, dimA=null}
   *   {dimB = world, dimA=foobar}
   *
   * When segment 1 and segment 2 are merged, dimA will not be present in segment 1 (no rows had values for dimA,
   * so dimA was not written to disk with segment 1).
   *
   * For segment 2, dimA did have non-null values, so when it was persisted to disk, "null" would be
   * assigned an encoded value (i.e., 0) within segment 2.
   *
   * The first row in both segments where dimB = hello should merge with each other.
   *
   * However, when rows from segment 1 and segment 2 are merged, the "missing" dimA values in segment 1 appear as
   * null values within the merging process; the null dimA values from segment 2 appear as a dictionary encoding of 0.
   *
   * Thus, the rows from segment 1 and segment 2 where dimA is missing/null will not merge properly.
   *
   * To correct this, the missing values for dimA in segment 1 will be converted to
   * encoded null values so that the rows can equate.
   *
   * @return true, if this conversion from missing->null is necessary
   *         false, otherwise.
   */
  public boolean needsConvertMissingValues();


  /**
   * If a dimension has no values after merging across segments, it does not need to be written to disk and can be skipped.
   *
   * @return true, if this dimension can be skipped
   *         false, if this dimension must be written
   */
  public boolean canSkip();


  /**
   * When merging values across segments when encoding is present, the encoded values within each segment
   * may need to be converted, based on the total ordering of actual values in the merged data set.
   *
   * This function converts encoded values from a single segment to its equivalent encoded value within the
   * final, sorted merged data set.
   *
   * @param value        Value to convert
   * @param indexNumber  The index number of the segment/adapter where the to-be-converted value originated.
   *
   * @return converted encoded value
   */
  public EncodedType getConvertedEncodedValue(EncodedType value, int indexNumber);


  /**
   * IndexMerger and IndexMergerV9 persist their columns in a different format.
   *
   * setWriteV8() instructs the DimensionMerger to operate in "V8" mode, i.e. in the manner of IndexMerger,
   * instead of IndexMergerV9.
   *
   * If using V8, this function MUST be called before any other functions on the DimensionMerger.
   */
  public void setWriteV8();


  /**
   * V8 version of closeWriters().
   *
   * @param rowNumConversions  Same as with closeWriters()
   * @param invertedOut        Output file for inverted indexes (e.g., bitmaps)
   * @param spatialOut         Output file for spatial indexes
   * @throws IOException
   */
  public void closeWritersV8(List<IntBuffer> rowNumConversions,
                             OutputSupplier<FileOutputStream> invertedOut,
                             OutputSupplier<FileOutputStream> spatialOut) throws IOException;

}
