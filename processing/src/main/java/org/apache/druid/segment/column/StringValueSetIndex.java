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
import java.util.SortedSet;

/**
 * Index on individual values, and provides bitmaps for the rows which contain these values
 */
public interface StringValueSetIndex
{
  /**
   * Get the {@link ImmutableBitmap} corresponding to the supplied value
   */
  BitmapColumnIndex forValue(@Nullable String value);

  /**
   * Get an {@link Iterable} of {@link ImmutableBitmap} corresponding to the specified set of values (if they are
   * contained in the underlying column). The set must be sorted using
   * {@link org.apache.druid.java.util.common.guava.Comparators#naturalNullsFirst()}.
   */
  BitmapColumnIndex forSortedValues(SortedSet<String> values);
}
