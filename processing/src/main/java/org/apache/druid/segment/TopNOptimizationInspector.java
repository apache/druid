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

package org.apache.druid.segment;

/**
 * oddly specific {@link Segment} inspector for topN optimizations
 */
public interface TopNOptimizationInspector
{
  /**
   * Returns true if this storage adapter can filter some rows out. The actual column cardinality can be lower than
   * what {@link DimensionDictionarySelector#getValueCardinality()} returns if this returns true. Dimension selectors
   * for such storage adapter can return non-contiguous dictionary IDs because the dictionary IDs in filtered rows
   * will not be returned. Note that the number of rows accessible via this storage adapter will not necessarily
   * decrease because of the built-in filters. For inner joins, for example, the number of joined rows can be larger
   * than the number of rows in the base adapter even though this method returns true.
   */
  boolean isFiltered();
}
