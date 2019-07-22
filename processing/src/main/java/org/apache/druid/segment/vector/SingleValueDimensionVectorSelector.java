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

package org.apache.druid.segment.vector;

import org.apache.druid.segment.DimensionDictionarySelector;

/**
 * Vectorized selector for a singly-valued string-typed column. Unlike the non-vectorized version, this is done as
 * a separate interface, which is useful since it allows "getRowVector" to be a primitive int array.
 *
 * @see org.apache.druid.segment.DimensionSelector, the non-vectorized version.
 * @see MultiValueDimensionVectorSelector, the multi-valued version.
 */
public interface SingleValueDimensionVectorSelector extends DimensionDictionarySelector, VectorSizeInspector
{
  /**
   * Get the current vector. The array will be reused, so it is not a good idea to retain a reference to it.
   */
  int[] getRowVector();
}
