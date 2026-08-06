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

package org.apache.druid.query.topn.vector;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.groupby.epinephelinae.GroupingSelector;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;

import javax.annotation.Nullable;

/**
 * Column processor for topN dimensions in the vectorized execution path.
 *
 * Writes dimension keys into shared memory in the same format as
 * {@link org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnSelector}, so that a
 * {@link org.apache.druid.query.groupby.epinephelinae.VectorGrouper} can be used for aggregation. After all rows in a
 * bucket have been aggregated, {@link #getDimensionValue(MemoryPointer, int)} decodes a key back to the raw dimension
 * value for insertion into a {@link org.apache.druid.query.topn.TopNResultBuilder}.
 *
 * @see TopNVectorColumnProcessorFactory
 * @see org.apache.druid.query.topn.types.TopNColumnAggregatesProcessor the non-vectorized equivalent
 */
public interface TopNVectorColumnSelector extends GroupingSelector
{
  /**
   * Size in bytes of the key written for each row by this selector.
   */
  int getGroupingKeySize();

  /**
   * Write dimension keys for rows [startRow, endRow) from the current vector into keySpace at keyOffset.
   */
  void writeKeys(WritableMemory keySpace, int keySize, int keyOffset, int startRow, int endRow);

  /**
   * Decode the key at keyMemory[keyOffset] back to a dimension value.
   *
   * @return the raw dimension value, or null for null dimension keys
   */
  @Nullable
  Object getDimensionValue(MemoryPointer keyMemory, int keyOffset);

  /**
   * Reset any internal state, such as a locally-built dictionary. Must be called between processing buckets when
   * a dictionary-building selector is in use.
   */
  void reset();
}
