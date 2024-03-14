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

/**
 * Converts back the dictionaryId to the dimension value. The implementations might or might not handle
 * {@link GroupByColumnSelectorStrategy#GROUP_BY_MISSING_VALUE}. The callers should handle those values appropriately on
 * their own, and filter those out before trying to convert the dictionary id back to value.
 *
 * The encoding - decoding workflow looks like:
 *
 * Encoding
 * 1. {@link DimensionToIdConverter} extracts the multi-value holder for the given row, which get's stored somewhere
 * 2. For each entry in the multi-value object, the value gets encoded into a dictionaryId, using {@link DimensionToIdConverter#getIndividualValueDictId}
 * 3. The callers can use this integer dictionaryID to materialize the results somewhere
 *
 * Decoding
 * 1. The materialized dictionary id is deserialized back to an int, and then decoded into value using {@link #idToKey}
 *
 * @see DimensionToIdConverter for converting the dimensions to dictionary ids
 *
 * @param <DimensionType> Type of the dimension's values
 */
public interface IdToDimensionConverter<DimensionType>
{
  /**
   * Decodes the dictionaryId back to the dimensionKey
   */
  DimensionType idToKey(int id);

  /**
   * Returns if the object comparison can be optimised directly by comparing the dictionaryIds, instead of decoding the
   * objects and comparing those. Therefore, it returns true iff the "dict" function defined by dict(id) = value is
   * monotonically increasing.
   *
   * Ids backed by dictionaries built on the fly can never be compared, therefore those should always return false.
   */
  boolean canCompareIds();
}
