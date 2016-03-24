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

import io.druid.segment.data.Indexed;


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
 * @param <EncodedType> class of the encoded values
 * @param <ActualType> class of the actual values
 *
 */
public interface DimensionIndexer<EncodedType extends Comparable<EncodedType>, ActualType extends Comparable<ActualType>>
{

  /**
   * Given a list of row values (for multivalued dimensions) or single row value, update any
   * internal data structures with the ingested values and return the row values as a Comparable[]
   * to be used as a TimeAndDims key.
   *
   * The values within the Comparable[] return value should be encoded if applicable, i.e. as instances of EncodedType.
   *
   * NOTE: This function can change the internal state of the DimensionIndexer.
   *
   * @param dimValues Row value(s) to process
   *
   * @return A new DimensionIndexer object that synchronizes on the provided lock.
   */
  public Comparable[] processRowValsToIndexKey(Object dimValues);


  /**
   * Some rows may not contain a particular dimension. Suppose the schema has DimA and DimB.
   * If an event {"dimB"="abcd"} appears, dimA's DimensionIndexer will not see that null value for DimA.
   * This function is called by IncrementalIndexAdapter to inform the DimensionIndexer of such unseen null values.
   */
  public void addNullLookup();


  /**
   * Given an encoded value, return its full representation.
   *
   * Using the example in the class description:
   *   getActualValue(0, true) would return "Apple"
   *   getActualValue(0, false) would return "Hello"
   *
   * @param intermediateValue The encoded value to be converted
   * @param idSorted true, if the encoded value was sorted by actual value
   *                 false, if the encoded value was ordered by time of ingestion
   *
   * @return The actual value associated with the encoded intermediateValue
   *
   */
  public ActualType getActualValue(EncodedType intermediateValue, boolean idSorted);


  /**
   * Given an actual value, return its encoded form.
   *
   * Using the example in the class description:
   *   getEncodedValue("Apple", true) would return 0
   *   getEncodedValue("Apple", false) would return 2
   *
   * @param fullValue Actual value to be converted.
   * @param idSorted  true, if return value should be an encoded value ordered by associated actual value
   *                  false, if return value should be an encoded value ordered by time of ingestion
   * @return An encoded value associated with fullValue
   */
  public EncodedType getEncodedValue(ActualType fullValue, boolean idSorted);


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
   * row values seen in calls to processRowValsToIndexKey().
   *
   * However, on a disk-backed segment (QueryableIndex), the numeric dimensions do not currently have any
   * supporting index structures that can be used to efficiently determine min/max values. A full column scan would
   * be needed, or a change to the segment file that stores the min/max values previously determined
   * during ingestion.
   *
   * To maintain consistency between in-memory/on-disk semantics, getMinValue()/getMaxValue() will return
   * null for both cases on numeric dimensions.
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

}
