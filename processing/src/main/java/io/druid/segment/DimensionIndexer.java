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

import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.data.Indexed;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;


/**
 * Processing related interface
 *
 * A DimensionIndexer is a per-dimension stateful object that encapsulates type-specific operations and data structures
 * used during the in-memory ingestion process (i.e., work done by IncrementalIndex).
 *
 * Ingested row values are passed to a DimensionIndexer, which will update its internal data structures such as
 * a value->ID dictionary as row values are seen.
 *
 * The DimensionIndexer is also responsible for implementing various value lookup operations,
 * such as conversion between an encoded value and its full representation. It maintains knowledge of the
 * mappings between encoded values and actual values.
 *
 * NOTE:
 * When encoding is present, there are two relevant orderings for the encoded values.
 *
 * 1.) Ordering based on encoded value's order of ingestion
 * 2.) Ordering based on converted actual value
 *
 * Suppose we have a new String dimension DimA, which sees the values "Hello", "World", and "Apple", in that order.
 * This would correspond to dictionary encodings of "Hello"=0, "World"=1, and "Apple"=2, by the order
 * in which these values were first seen during ingestion.
 *
 * However, some use cases require the encodings to be sorted by their associated actual values.
 * In this example, that ordering would be "Apple"=0, "Hello"=1, "World"=2.
 *
 * The first ordering will be referred to as "Unsorted" in the documentation for this interface, and
 * the second ordering will be referred to as "Sorted".
 *
 * @param <EncodedType> class of the encoded values
 * @param <ActualType> class of the actual values
 *
 */
public interface DimensionIndexer<EncodedType extends Comparable<EncodedType>, ActualType extends Comparable<ActualType>>
{
  /**
   * Given a single row value, update any internal data structures with the ingested values and
   * return the row values as an array to be used as a TimeAndDims key.
   *
   * For example, the dictionary-encoded String-type column will return an int[] containing a dictionary ID.
   *
   * The value within the returned array should be encoded if applicable, i.e. as instances of EncodedType.
   *
   * NOTE: This function can change the internal state of the DimensionIndexer.
   *
   * @param dimValues Single row val to process
   *
   * @return An array containing an encoded representation of the input row value.
   */
  public Object processSingleRowValToIndexKey(Object dimValues);


  /**
   * Given a list of row values (for multivalued dimensions), update any internal data structures with the ingested
   * values and return the row values as an array to be used as a TimeAndDims key.
   *
   * For example, the dictionary-encoded String-type column will return an int[] containing dictionary IDs.
   *
   * The values within the returned array should be encoded if applicable, i.e. as instances of EncodedType.
   *
   * NOTE: This function can change the internal state of the DimensionIndexer.
   *
   * @param dimValuesList List of row values to process
   * @return An array containing encoded representations of the input row values
   */
  public Object processRowValsListToIndexKey(Object dimValuesList);


  /**
   * Given an encoded value that was ordered by time of ingestion, return the equivalent
   * encoded value ordered by associated actual value.
   *
   * Using the example in the class description:
   *   getSortedEncodedValueFromUnsorted(0) would return 2
   *
   * @param unsortedIntermediateValue value to convert
   * @return converted value
   */
  public EncodedType getSortedEncodedValueFromUnsorted(EncodedType unsortedIntermediateValue);


  /**
   * Given an encoded value that was ordered by associated actual value, return the equivalent
   * encoded value ordered by time of ingestion.
   *
   * Using the example in the class description:
   *   getUnsortedEncodedValueFromSorted(2) would return 0
   *
   * @param sortedIntermediateValue value to convert
   * @return converted value
   */
  public EncodedType getUnsortedEncodedValueFromSorted(EncodedType sortedIntermediateValue);


  /**
   * Returns an indexed structure of this dimension's sorted actual values.
   * The integer IDs represent the ordering of the sorted values.
   *
   * Using the example in the class description:
   *  "Apple"=0,
   *  "Hello"=1,
   *  "World"=2
   *
   * @return Sorted index of actual values
   */
  public Indexed<ActualType> getSortedIndexedValues();


  /**
   * Get the minimum dimension value seen by this indexer.
   *
   * NOTE:
   * On an in-memory segment (IncrementaIndex), we can determine min/max values by looking at the stream of
   * row values seen in calls to processSingleRowValToIndexKey().
   *
   * However, on a disk-backed segment (QueryableIndex), the numeric dimensions do not currently have any
   * supporting index structures that can be used to efficiently determine min/max values.
   *
   * When numeric dimension support is added, the segment format should be changed to store min/max values, to
   * avoid performing a full-column scan to determine these values for numeric dims.
   *
   * @return min value
   */
  public ActualType getMinValue();


  /**
   * Get the maximum dimension value seen by this indexer.
   *
   * @return max value
   */
  public ActualType getMaxValue();


  /**
   * Get the cardinality of this dimension's values.
   *
   * @return value cardinality
   */
  public int getCardinality();


  /**
   * Return a DimensionSelector, an object used to read rows from a StorageAdapter's Cursor.
   *
   * See StringDimensionIndexer.makeDimensionSelector() for a reference implementation.
   *
   * @param spec Specifies the output name of a dimension and any extraction functions to be applied.
   * @param currEntry Provides access to the current TimeAndDims object in the Cursor
   * @param desc Descriptor object for this dimension within an IncrementalIndex
   * @return A new DimensionSelector that reads rows from currEntry
   */
  public DimensionSelector makeDimensionSelector(
      DimensionSpec spec,
      IncrementalIndexStorageAdapter.EntryHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  );


  /**
   * Compares the row values for this DimensionIndexer's dimension from a TimeAndDims key.
   *
   * The dimension values within a TimeAndDims key always use the "unsorted" ordering for encoded values.
   *
   * The row values are passed to this function as an Object, the implementer should cast them to the type
   * appropriate for this dimension.
   *
   * For example, a dictionary encoded String implementation would cast the Objects as int[] arrays.
   *
   * When comparing, if the two arrays have different lengths, the shorter array should be ordered first.
   *
   * Otherwise, the implementer of this function should iterate through the unsorted encoded values, converting
   * them to their actual type (e.g., performing a dictionary lookup for a dict-encoded String dimension),
   * and comparing the actual values until a difference is found.
   *
   * Refer to StringDimensionIndexer.compareTimeAndDimsKey() for a reference implementation.
   *
   * @param lhs dimension value array from a TimeAndDims key
   * @param rhs dimension value array from a TimeAndDims key
   * @return comparison of the two arrays
   */
  public int compareTimeAndDimsKey(Object lhs, Object rhs);


  /**
   * Check if two row value arrays from TimeAndDims keys are equal.
   *
   * @param lhs dimension value array from a TimeAndDims key
   * @param rhs dimension value array from a TimeAndDims key
   * @return true if the two arrays are equal
   */
  public boolean checkTimeAndDimsKeyEqual(Object lhs, Object rhs);


  /**
   * Given a row value array from a TimeAndDims key, generate a hashcode.
   * @param key dimension value array from a TimeAndDims key
   * @return hashcode of the array
   */
  public int getTimeAndDimsKeyHashcode(Object key);


  /**
   * Given a row value array from a TimeAndDims key, as described in the documentatiion for compareTimeAndDimsKey(),
   * convert the unsorted encoded values to an array of actual values.
   *
   * @param key dimension value array from a TimeAndDims key
   * @return array containing the actual values corresponding to the encoded values in the input array
   */
  public Object convertTimeAndDimsKeyToActualArray(Object key);


  /**
   * Given a row value array from a TimeAndDims key, as described in the documentatiion for compareTimeAndDimsKey(),
   * convert the unsorted encoded values to an array of sorted encoded values (i.e., sorted by their corresponding actual values)
   *
   * @param key dimension value array from a TimeAndDims key
   * @return array containing the sorted encoded values corresponding to the unsorted encoded values in the input array
   */
  public Object convertTimeAndDimsKeyToSortedEncodedArray(Object key);


  /**
   * Helper function for building bitmap indexes for integer-encoded dimensions.
   *
   * Called by IncrementalIndexAdapter as it iterates through its sequence of rows.
   *
   * Given a row value array from a TimeAndDims key, with the current row number indicated by "rowNum",
   * set the index for "rowNum" in the bitmap index for each value that appears in the row value array.
   *
   * For example, if key is an int[] array with values [1,3,4] for a dictionary-encoded String dimension,
   * and rowNum is 27, this function would set bit 27 in bitmapIndexes[1], bitmapIndexes[3], and bitmapIndexes[4]
   *
   * See StringDimensionIndexer.fillBitmapsFromTimeAndDimsKey() for a reference implementation.
   *
   * If a dimension type does not support bitmap indexes, this function will not be called
   * and can be left unimplemented.
   *
   * @param key dimension value array from a TimeAndDims key
   * @param rowNum current row number
   * @param bitmapIndexes array of bitmaps, indexed by integer dimension value
   * @param factory bitmap factory
   */
  public void fillBitmapsFromTimeAndDimsKey(Object key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory);


  /**
   * Return a Predicate that accepts a row value array Object from a TimeAndDims key, as described in the documentation
   * for compareTimeAndDimsKey(), and matches the row value array against matchValue.
   *
   * See StringDimensionIndexer.makeTimeAndDimsKeyValueMatchPredicate() for a reference implementation.
   *
   * @param matchValue value to match on
   * @return A predicate that matches a dimension value array from a TimeAndDims key against "matchValue"
   */
  public Predicate makeTimeAndDimsKeyValueMatcherPredicate(Comparable matchValue);


  /**
   * Return a Predicate that accepts a row value array Object from a TimeAndDims key, as described in the documentation
   * for compareTimeAndDimsKey(). The returned predicate should iterate through the row value array and
   * apply the "basePredicate" to each value until a match is found or the row values are exhausted.
   *
   * See StringDimensionIndexer.makeTimeAndDimsKeyValueMatchPredicate() for a reference implementation.
   *
   * @param basePredicate predicate to be applied to each row value
   * @return A predicate that matches a dimension value array from a TimeAndDims key against "matchValue"
   */
  public Predicate makeTimeAndDimsKeyValueMatcherPredicate(Predicate basePredicate);
}
