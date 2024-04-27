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

package org.apache.druid.segment.index.semantic;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.index.BitmapColumnIndex;

import javax.annotation.Nullable;

/**
 * Construct a {@link BitmapColumnIndex} for any array element which might be present in an array contained in the
 * column.
 */
public interface ArrayElementIndexes
{
  /**
   * Get the {@link ImmutableBitmap} corresponding to rows with array elements matching the supplied value.  Generates
   * an empty bitmap when passed a value that doesn't exist in any array. May return null if a value index cannot be
   * computed for the supplied value type.
   *
   * @param value       value to match against any array element in a row
   * @param valueType   type of the value to match, used to assist conversion from the match value type to the column
   *                    value type
   * @return            {@link ImmutableBitmap} corresponding to the rows with array elements which match the value, or
   *                    null if an index connot be computed for the supplied value type
   */
  @Nullable
  BitmapColumnIndex containsValue(@Nullable Object value, TypeSignature<ValueType> valueType);
}
