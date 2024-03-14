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

package org.apache.druid.query.groupby.epinephelinae.column;

import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

/**
 * Interface for converters of dimension to dictionary id.
 *
 * This is a slightly convoluted interface because it also encapsulates the additional logic for handling multi-value
 * dimensions. It has an additional step that converts the given dimensions to "dimension holders", which represent the
 * multi-value holders for a given dimension.
 * Therefore, the conversion goes from ColumnValueSelector -> DimensionHolder -> DictionaryID (for each dimension in the holder)
 *
 * The dimension holder is only applicable for multi-value strings.
 * For other dimensions that cannot have multi-values the dimension holder is identical to the dimension. They can be
 * defensively cast or homogenised, for example doubles to floats for float selectors or Long[] to Object[] for array
 * selectors, so that the upstream callers can assume the class of the dimensions. The size of these dimensions is always 1,
 * and only contain a value at index 0.
 *
 * Converting a value to its dictionary id might require building dictionaries on the fly while computing the id. The
 * return type of the methods, except {@link #multiValueSize}, takes that into account.
 *
 * The implementations can pre-convert the value to the dictionaryId while extracting the dimensionHolder. Extracting
 * dictionary id for a specific value from the (potentially multi-value dimension holder) can be done by calling
 * {@link #getIndividualValueDictId} and passing the index to the multi-value.
 *
 * @see IdToDimensionConverter for converting the dictionary values back to dimensions
 *
 * @param <DimensionHolderType> Type of the dimension holder
 */
public interface DimensionToIdConverter<DimensionHolderType>
{
  /**
   * @param selector Column value selector to extract the dimension holder from
   * @param reusableValue Dimension holder can be reused throughout multiple calls to prevent reallocation of memory
   *                      or arrays. The older value can be disregarded and the object can be reused for freely by this call.
   * @return DimensionHolder associated with the selector, and the internal dictionary increase associated with it
   */
  MemoryEstimate<DimensionHolderType> getMultiValueHolder(
      ColumnValueSelector selector,
      // TODO(laksh): This is always null. Find a way to use this or remove this parameter
      @Nullable DimensionHolderType reusableValue
  );

  /**
   * @param multiValueHolder Multi value holder obtained from call to {@link #getMultiValueHolder}
   * @return Size of the multi-value dimension
   */
  int multiValueSize(DimensionHolderType multiValueHolder);

  /**
   * @param multiValueHolder Multi value holder obtained from call to {@link #getMultiValueHolder}
   * @param index Index of the value inside the multi-value holder to obtain
   * @return DictionaryId of the object at the given index
   */
  MemoryEstimate<Integer> getIndividualValueDictId(DimensionHolderType multiValueHolder, int index);
}
