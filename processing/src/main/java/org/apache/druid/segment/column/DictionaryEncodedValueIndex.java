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

package org.apache.druid.segment.column;

import org.apache.druid.collections.bitmap.ImmutableBitmap;

/**
 * This exposes a 'raw' view into bitmap value indexes for {@link DictionaryEncodedColumn}. This allows callers
 * to directly retrieve bitmaps via dictionary ids.
 *
 * This interface should only be used when it is beneficial to operate in such a manner; callers of this class must
 * either already know what value the dictionary id represents, not care at all, or have some other means to know
 * exactly which bitmaps to retrieve.
 *
 * Most filter implementations should likely be using higher level index instead, such as {@link StringValueSetIndex},
 * {@link LexicographicalRangeIndex}, {@link NumericRangeIndex}, or {@link DruidPredicateIndex}.
 */
public interface DictionaryEncodedValueIndex
{
  /**
   * Get the {@link ImmutableBitmap} for dictionary id of the underlying dictionary
   */
  ImmutableBitmap getBitmap(int idx);
}
