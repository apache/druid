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
 * Dimension to integer id encoder - decoder i.e. it is an interface for converters of dimension to dictionary id and back.
 * It only handles single value dimensions. Handle multi-value dimensions (i.e. strings) using the {@link KeyMappingMultiValueGroupByColumnSelectorStrategy}.
 * <p>
 * <strong>Encoding</strong><br />
 * The caller is expected to handle non-null values. Null values must be filtered by the caller, and assigned {@link GroupByColumnSelectorStrategy#GROUP_BY_MISSING_VALUE}
 * 1. {@link DimensionIdCodec} extracts the dimension from the selector
 * 2. The value gets encoded into a dictionaryId, using {@link DimensionIdCodec#lookupId}
 * 3. The callers can use this integer dictionaryID to represent the grouping key
 * <p>
 * <strong>Decoding</strong><br />
 * Converts back the dictionaryId to the dimension value. The implementations are not expected to handle {@link GroupByColumnSelectorStrategy#GROUP_BY_MISSING_VALUE}.
 * The callers should handle those values appropriately ontheir own, and filter those out before trying to convert
 * the dictionary id back to value.
 *
 * @param <DimensionType> Type of the dimension holder
 */
public interface DimensionIdCodec<DimensionType>
{
  /**
   * @return DictionaryId of the object at the given index and the memory increase associated with it
   */
  MemoryFootprint<Integer> lookupId(DimensionType dimension);

  DimensionType idToKey(int id);

  /**
   * Returns if the object comparison can be optimised directly by comparing the dictionaryIds, instead of decoding the
   * objects and comparing those. Therefore, it returns true iff the "dict" function defined by dict(id) = value is
   * monotonically increasing.
   *
   * Ids backed by dictionaries built on the fly can never be compared, therefore those should always return false.
   */
  boolean canCompareIds();

  void reset();
}
