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

/**
 */
public interface ColumnCapabilities
{
  ValueType getType();

  boolean isDictionaryEncoded();
  Capable areDictionaryValuesSorted();
  Capable areDictionaryValuesUnique();
  boolean isRunLengthEncoded();
  boolean hasBitmapIndexes();
  boolean hasSpatialIndexes();
  boolean hasMultipleValues();
  boolean isFilterable();

  /**
   * This property indicates that this {@link ColumnCapabilities} is "complete" in that all properties can be expected
   * to supply valid responses. This is mostly a hack to work around {@link ColumnCapabilities} generators that
   * fail to set {@link #hasMultipleValues()} even when the associated column really could have multiple values.
   * Until this situation is sorted out, if this method returns false, callers are encouraged to ignore
   * {@link #hasMultipleValues()} and treat that property as if it were unknown.
   *
   * todo: replace all booleans with {@link Capable} and this method can be dropped
   */
  boolean isComplete();


  enum Capable
  {
    FALSE,
    TRUE,
    UNKNOWN;

    public boolean isTrue()
    {
      return this == TRUE;
    }

    public Capable and(Capable other)
    {
      if (this == UNKNOWN || other == UNKNOWN) {
        return UNKNOWN;
      }
      return this == TRUE && other == TRUE ? TRUE : FALSE;
    }

    public static Capable of(boolean bool)
    {
      return bool ? TRUE : FALSE;
    }
  }
}
