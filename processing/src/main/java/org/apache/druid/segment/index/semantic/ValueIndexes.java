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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Construct a {@link BitmapColumnIndex} for a specific value which might be present in the column.
 */
public interface ValueIndexes
{

  /**
   * Get a {@link BitmapColumnIndex} which can compute the {@link ImmutableBitmap} corresponding to rows matching the
   * supplied value.  Generates an empty bitmap when passed a value that doesn't exist. May return null if a value
   * index cannot be computed for the supplied value type.
   * <p>
   * Does not match null, use {@link NullValueIndex} for matching nulls.
   *
   * @param value       value to match
   * @param valueType   type of the value to match, used to assist conversion from the match value type to the column
   *                    value type
   * @return            {@link ImmutableBitmap} corresponding to the rows which match the value, or null if an index
   *                    connot be computed for the supplied value type
   */
  @Nullable
  BitmapColumnIndex forValue(@Nonnull Object value, TypeSignature<ValueType> valueType);
}
