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

import javax.annotation.Nullable;

/**
 * This exposes a 'raw' view into a bitmap value indexes of a string {@link DictionaryEncodedColumn}, allowing
 * operation via dictionaryIds, as well as access to lower level details of such a column like value lookup and
 * value cardinality.
 *
 * This interface should only be used when it is beneficial to operate in such a manner, most filter implementations
 * should likely be using {@link StringValueSetIndex}, {@link DruidPredicateIndex}, {@link LexicographicalRangeIndex} or
 * some other higher level index instead.
 */
public interface DictionaryEncodedStringValueIndex
{
  boolean hasNulls();
  /**
   * Get the cardinality of the underlying value dictionary
   */
  int getCardinality();

  /**
   * Get the value in the underlying value dictionary of the specified dictionary id
   */
  @Nullable
  String getValue(int index);

  /**
   * Returns the index of "value" in this DictionaryEncodedStringValueIndex, or a negative value, if not present in
   * the underlying dictionary
   */
  int getIndex(@Nullable String value);

  /**
   * Get the {@link ImmutableBitmap} for dictionary id of the underlying dictionary
   */
  ImmutableBitmap getBitmap(int idx);
}
