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
 * Interface for converters of dimension to dictionary id.
 *
 * It only handles single-value dimensions. Handle multi-value dimensions (i.e. strings) using the
 * {@link KeyMappingMultiValueGroupByColumnSelectorStrategy}
 *
 * @see IdToDimensionConverter for converting the dictionary values back to dimensions
 *
 * @param <DimensionType> Type of the dimension holder
 */
public interface DimensionToIdConverter<DimensionType>
{
  /**
   * @return DictionaryId of the object at the given index and the memory increase associated with it
   */
  MemoryEstimate<Integer> lookupId(DimensionType multiValueHolder);
}
